package io.github.gliter.flink.avro;

import io.github.gliter.flink.avro.testutil.FileCollectingSink;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.specific.SpecificRecord;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.BufferedOutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies a v1 generated specific-record savepoint restores with a v2 generated job artifact.
 */
class SpecificRecordSavepointIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(SpecificRecordSavepointIT.class);
    private static final Schema V1 = new Schema.Parser().parse("""
            {"type":"record","name":"Customer","namespace":"state",
             "fields":[{"name":"name","type":"string"}]}
            """);
    private static final Schema V2 = new Schema.Parser().parse("""
            {"type":"record","name":"Customer","namespace":"state",
             "fields":[{"name":"name","type":"string"},
                       {"name":"tier","type":"string","default":"standard"}]}
            """);

    @Test
    void restoresSpecificRecordStateFromV1JobWithV2GeneratedClass() throws Exception {
        Configuration miniClusterConfiguration = new Configuration();
        miniClusterConfiguration.set(CoreOptions.TMP_DIRS, flinkTempDirectory());
        try (GeneratedArtifact v1 = generatedArtifact(V1);
             GeneratedArtifact v2 = generatedArtifact(V2)) {
            MiniCluster cluster = new MiniCluster(new MiniClusterConfiguration.Builder()
                    .setConfiguration(miniClusterConfiguration).setNumTaskManagers(1).setNumSlotsPerTaskManager(1).build());
            try {
                cluster.start();
                String registryUrl = "mock://flink-it-registry";
                Map<String, Object> writerConfig = Map.of("auto.register.schemas", true);
                Map<String, Object> migrationConfig = Map.of(
                        "auto.register.schemas", true,
                        "force.migration.nonce", 1);
                Path directory = Files.createTempDirectory("specific-record-savepoint");
                String savepoint = writeSavepoint(cluster, directory, v1,
                        new ValueStateDescriptor<>("customer", serializer(v1.recordClass(), writerConfig)));
                assertEquals("Ada|standard", restore(cluster, savepoint, v2, migrationConfig));
                assertSchemaRegistered(registryUrl);
            } finally {
                closeMiniCluster(cluster);
            }
        }
    }

    private static void assertSchemaRegistered(String registryUrl) throws Exception {
        Schema currentSchema = V2;
        try (SchemaRegistryClient client = SchemaRegistryClientFactory.newClient(registryUrl, 1000,
                List.of(new AvroSchemaProvider()), Map.of("schema.registry.url", registryUrl), Map.of())) {
            client.getId("customer-state", new AvroSchema(currentSchema), false);
        }
    }

    @Test
    void restoresDefaultAvroSerializerStateFromSpecificRecordClass() throws Exception {
        Configuration miniClusterConfiguration = new Configuration();
        miniClusterConfiguration.set(CoreOptions.TMP_DIRS, flinkTempDirectory());
        try (GeneratedArtifact v1 = generatedArtifact(V1);
             GeneratedArtifact v2 = generatedArtifact(V2)) {
            MiniCluster cluster = new MiniCluster(new MiniClusterConfiguration.Builder()
                    .setConfiguration(miniClusterConfiguration).setNumTaskManagers(1).setNumSlotsPerTaskManager(1).build());
            try {
                cluster.start();
                Map<String, Object> schemaRegistryConfig = Map.of("auto.register.schemas", true);
                Path directory = Files.createTempDirectory("specific-default-avro-savepoint");
                String savepoint = writeSavepoint(cluster, directory, v1, defaultAvroDescriptor(v1.recordClass()));
                assertEquals("Ada|standard", restore(cluster, savepoint, v2, schemaRegistryConfig));
            } finally {
                closeMiniCluster(cluster);
            }
        }
    }

    private static String writeSavepoint(MiniCluster cluster, Path directory, GeneratedArtifact artifact,
                                         ValueStateDescriptor<SpecificRecord> descriptor) throws Exception {
        Path events = eventFile("specific-write");
        StreamExecutionEnvironment env = environment(cluster, new Configuration(), artifact.jar());
        env.addSource(new OneThenWaitSource()).keyBy(value -> value)
                .process(new WriteState(descriptor, artifact.recordClass()))
                .uid("specific-customer-state").addSink(new FileCollectingSink<>(events));
        JobClient job = env.executeAsync("write-specific-v1");
        awaitEvent(events);
        Thread.sleep(1_000L);
        String savepoint = cluster.triggerSavepoint(job.getJobID(), directory.toUri().toString(), false,
                SavepointFormatType.CANONICAL).get(30, TimeUnit.SECONDS);
        job.cancel().get(30, TimeUnit.SECONDS);
        return savepoint;
    }

    private static String restore(MiniCluster cluster, String savepoint, GeneratedArtifact artifact,
                                  Map<String, Object> schemaRegistryConfig) throws Exception {
        Configuration config = new Configuration();
        config.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        StreamExecutionEnvironment env = environment(cluster, config, artifact.jar());
        Path events = eventFile("specific-restore");
        env.fromData(1).keyBy(value -> value)
                .process(new ReadState(serializer(artifact.recordClass(), schemaRegistryConfig)))
                .uid("specific-customer-state").addSink(new FileCollectingSink<>(events));
        JobClient job = env.executeAsync("restore-specific-v2");
        try {
            String restored = awaitEvent(events);
            job.getJobExecutionResult().get(30, TimeUnit.SECONDS);
            return restored;
        } finally {
            if (!job.getJobStatus().get().isGloballyTerminalState()) {
                job.cancel().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static StreamExecutionEnvironment environment(MiniCluster cluster, Configuration config, Path jobJar)
            throws Exception {
        URI rest = cluster.getRestAddress().get(30, TimeUnit.SECONDS);
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.setString("state.backend.rocksdb.localdir", rocksDbDirectory());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                rest.getHost(), rest.getPort(), config, jobJar.toString());
        env.setParallelism(1);
        return env;
    }

    private static String flinkTempDirectory() {
        return System.getProperty("flink.test.flink-tmp-dir");
    }

    private static String rocksDbDirectory() {
        return System.getProperty("flink.test.rocksdb-local-dir");
    }

    private static void closeMiniCluster(MiniCluster cluster) throws Exception {
        try {
            cluster.closeAsync().get(15, TimeUnit.SECONDS);
        } catch (TimeoutException exception) {
            LOGGER.warn("Timeout during mini cluster closing", exception);
        }
    }

    private static Path eventFile(String prefix) throws Exception {
        Path file = Files.createTempFile(prefix, ".txt");
        Files.writeString(file, "");
        return file;
    }

    private static String awaitEvent(Path file) throws Exception {
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(1, Files.readAllLines(file).size()));
        return Files.readAllLines(file).get(0);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static SchemaResolvingAvroSerializer<SpecificRecord> serializer(Class<? extends SpecificRecord> type,
                                                                          Map<String, Object> schemaRegistryConfig) {
        return (SchemaResolvingAvroSerializer) SchemaResolvingAvroSerializer.forSpecific(
                "customer-state", (Class) type, schemaRegistryConfig);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ValueStateDescriptor<SpecificRecord> defaultAvroDescriptor(Class<? extends SpecificRecord> type) {
        return new ValueStateDescriptor("customer", (Class) type);
    }

    private static final class WriteState extends KeyedProcessFunction<Integer, Integer, String> {
        private final ValueStateDescriptor<SpecificRecord> descriptor;
        private final Class<? extends SpecificRecord> type;
        private transient ValueState<SpecificRecord> state;

        private WriteState(ValueStateDescriptor<SpecificRecord> descriptor, Class<? extends SpecificRecord> type) {
            this.descriptor = descriptor;
            this.type = type;
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext context) throws Exception {
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Integer ignored, Context context, Collector<String> out) throws Exception {
            SpecificRecord record = type.getDeclaredConstructor().newInstance();
            record.put(0, "Ada");
            state.update(record);
            out.collect("written");
        }
    }

    private static final class ReadState extends KeyedProcessFunction<Integer, Integer, String> {
        private final SchemaResolvingAvroSerializer<SpecificRecord> serializer;
        private transient ValueState<SpecificRecord> state;

        private ReadState(SchemaResolvingAvroSerializer<SpecificRecord> serializer) {
            this.serializer = serializer;
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext context) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("customer", (TypeSerializer<SpecificRecord>) serializer));
        }

        @Override
        public void processElement(Integer ignored, Context context, Collector<String> out) throws Exception {
            SpecificRecord record = state.value();
            out.collect(record.get(0) + "|" + record.get(1));
        }
    }

    private static final class OneThenWaitSource implements SourceFunction<Integer> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> context) throws Exception {
            synchronized (context.getCheckpointLock()) {
                context.collect(1);
            }
            while (running) {
                Thread.sleep(20L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static GeneratedArtifact generatedArtifact(Schema schema) throws Exception {
        Path root = Files.createTempDirectory("specific-record-job");
        Path sources = root.resolve("sources");
        Path classes = root.resolve("classes");
        Files.createDirectories(sources);
        Files.createDirectories(classes);
        new SpecificCompiler(schema).compileToDestination(sources.toFile(), sources.toFile());
        List<String> javaSources;
        try (Stream<Path> paths = Files.walk(sources)) {
            javaSources = paths.filter(path -> path.toString().endsWith(".java")).map(Path::toString).toList();
        }
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        List<String> args = new ArrayList<>(List.of("-classpath", System.getProperty("java.class.path"), "-d", classes.toString()));
        args.addAll(javaSources);
        if (compiler == null || compiler.run(null, null, null, args.toArray(String[]::new)) != 0)
            throw new IllegalStateException("Could not compile generated Avro record");
        Path jar = root.resolve("customer.jar");
        try (JarOutputStream out = new JarOutputStream(new BufferedOutputStream(Files.newOutputStream(jar, StandardOpenOption.CREATE)))) {
            try (Stream<Path> paths = Files.walk(classes)) {
                for (Path path : paths.filter(Files::isRegularFile).toList()) {
                    out.putNextEntry(new JarEntry(classes.relativize(path).toString().replace('\\', '/')));
                    Files.copy(path, out);
                    out.closeEntry();
                }
            }
        }
        URLClassLoader loader = new URLClassLoader(new URL[]{classes.toUri().toURL()}, SpecificRecordSavepointIT.class.getClassLoader());
        @SuppressWarnings("unchecked") Class<? extends SpecificRecord> type = (Class<? extends SpecificRecord>) Class.forName("state.Customer", true, loader);
        return new GeneratedArtifact(loader, type, jar);
    }

    private record GeneratedArtifact(URLClassLoader loader, Class<? extends SpecificRecord> recordClass,
                                     Path jar) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            loader.close();
        }
    }
}
