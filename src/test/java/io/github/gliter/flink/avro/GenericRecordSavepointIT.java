package io.github.gliter.flink.avro;

import io.github.gliter.flink.avro.testutil.FileCollectingSink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkException;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** A real Flink savepoint written with v1 state and restored by a v2 reader schema. */
class GenericRecordSavepointIT {
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
    void restoresOldStateUsingNewSchemaWithoutStateMigration() throws Exception {
        Path savepointDirectory = Files.createTempDirectory("confluent-avro-savepoint");
        Configuration miniClusterConfiguration = new Configuration();
        miniClusterConfiguration.set(CoreOptions.TMP_DIRS, flinkTempDirectory());
        Map<String, Object> schemaRegistryConfig = Map.of("auto.register.schemas", true);
        MiniCluster cluster = new MiniCluster(new MiniClusterConfiguration.Builder()
                .setConfiguration(miniClusterConfiguration)
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(1)
                .build());
        try {
            cluster.start();
            String savepoint = writeV1Savepoint(cluster, savepointDirectory, schemaRegistryConfig);
            String restoredName = restoreWithV2(cluster, savepoint, schemaRegistryConfig);
            assertEquals("Ada|standard", restoredName);
        } finally {
            closeMiniCluster(cluster);
        }
    }

    @Test
    void forcesRewriteOfConfluentAndRegularAvroSavepointsToCurrentSchema() throws Exception {
        Path savepointDirectory = Files.createTempDirectory("forced-avro-savepoint");
        Configuration miniClusterConfiguration = new Configuration();
        miniClusterConfiguration.set(CoreOptions.TMP_DIRS, flinkTempDirectory());
        String registryUrl = "mock://flink-it-registry";
        Map<String, Object> writerConfig = Map.of("auto.register.schemas", true);
        Map<String, Object> migrationConfig = Map.of(
                "auto.register.schemas", true,
                "force.migration.nonce", 1);
        MiniCluster cluster = new MiniCluster(new MiniClusterConfiguration.Builder()
                .setConfiguration(miniClusterConfiguration).setNumTaskManagers(1).setNumSlotsPerTaskManager(1).build());
        try {
            cluster.start();
            String confluentSavepoint = writeV1Savepoint(cluster, savepointDirectory, writerConfig);
            assertEquals("Ada|standard", restoreWithV2(cluster, confluentSavepoint, migrationConfig));
            assertSchemaRegistered(registryUrl);

            Map<String, Object> regularMigrationConfig = Map.of(
                    "auto.register.schemas", true,
                    "force.migration.nonce", 1);
            String regularAvroSavepoint = writeRegularFlinkAvroSavepoint(cluster, savepointDirectory);
            assertEquals("Ada|standard", restoreWithV2(cluster, regularAvroSavepoint, regularMigrationConfig));
            assertSchemaRegistered(registryUrl);
        } finally {
            closeMiniCluster(cluster);
        }
    }

    private static void assertSchemaRegistered(String registryUrl) throws Exception {
        try (SchemaRegistryClient client = SchemaRegistryClientFactory.newClient(registryUrl, 1000,
                List.of(new AvroSchemaProvider()), Map.of("schema.registry.url", registryUrl), Map.of())) {
            client.getId("customer-state", new AvroSchema(V2), false);
        }
    }

    @Test
    void restoresRegularFlinkAvroSavepointWithConfluentSerializer() throws Exception {
        Path savepointDirectory = Files.createTempDirectory("legacy-avro-savepoint");
        Configuration miniClusterConfiguration = new Configuration();
        miniClusterConfiguration.set(CoreOptions.TMP_DIRS, flinkTempDirectory());
        Map<String, Object> schemaRegistryConfig = Map.of("auto.register.schemas", true);
        MiniCluster cluster = new MiniCluster(new MiniClusterConfiguration.Builder()
                .setConfiguration(miniClusterConfiguration).setNumTaskManagers(1).setNumSlotsPerTaskManager(1).build());
        try {
            cluster.start();
            String savepoint = writeRegularFlinkAvroSavepoint(cluster, savepointDirectory);
            assertEquals("Ada|standard", restoreWithV2(cluster, savepoint, schemaRegistryConfig));
        } finally {
            closeMiniCluster(cluster);
        }
    }

    private static String writeV1Savepoint(MiniCluster cluster, Path savepointDirectory,
            Map<String, Object> schemaRegistryConfig) throws Exception {
        return writeSavepoint(cluster, savepointDirectory,
                new ValueStateDescriptor<>("customer",
                        SchemaResolvingAvroSerializer.forGeneric("customer-state", V1, schemaRegistryConfig)));
    }

    private static String writeSavepoint(MiniCluster cluster, Path savepointDirectory,
            ValueStateDescriptor<GenericRecord> descriptor) throws Exception {
        return writeStateSavepoint(cluster, savepointDirectory, new WriteState(descriptor));
    }

    private static String writeRegularFlinkAvroSavepoint(MiniCluster cluster, Path savepointDirectory)
            throws Exception {
        return writeSavepoint(cluster, savepointDirectory,
                new ValueStateDescriptor<>("customer", new GenericRecordAvroTypeInfo(V1)));
    }

    private static String writeStateSavepoint(MiniCluster cluster, Path savepointDirectory, WriteState writeState)
            throws Exception {
        Path events = Files.createTempFile("confluent-avro-write-events", ".txt");
        Files.writeString(events, "");
        StreamExecutionEnvironment environment = baseEnvironment(cluster, new Configuration());
        environment.addSource(new OneThenWaitSource())
                .keyBy(value -> value)
                .process(writeState)
                .uid("customer-state-operator")
                .addSink(new FileCollectingSink<>(events));
        JobClient job = environment.executeAsync("write-v1-state");
        awaitEvent(events);
        // The source has emitted, but the downstream chained operator may still be deploying.
        Thread.sleep(1_000L);
        String savepoint = cluster.triggerSavepoint(job.getJobID(), savepointDirectory.toUri().toString(), false,
                SavepointFormatType.CANONICAL).get(30, TimeUnit.SECONDS);
        job.cancel().get(30, TimeUnit.SECONDS);
        return savepoint;
    }

    private static String restoreWithV2(MiniCluster cluster, String savepoint,
            Map<String, Object> schemaRegistryConfig) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        StreamExecutionEnvironment environment = baseEnvironment(cluster, configuration);
        SchemaResolvingAvroSerializer<GenericRecord> serializer = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", V2, schemaRegistryConfig);
        Path events = Files.createTempFile("confluent-avro-restore-events", ".txt");
        Files.writeString(events, "");
        environment.fromData(1)
                .keyBy(value -> value)
                .process(new ReadState(serializer))
                .uid("customer-state-operator")
                .addSink(new FileCollectingSink<>(events));
        JobClient job = environment.executeAsync("restore-confluent-v2");
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

    private static StreamExecutionEnvironment baseEnvironment(MiniCluster cluster, Configuration configuration) throws Exception {
        URI restAddress = cluster.getRestAddress().get(30, TimeUnit.SECONDS);
        configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        configuration.set(PipelineOptions.GENERIC_TYPES, false);
        configuration.setString("state.backend.rocksdb.localdir", rocksDbDirectory());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createRemoteEnvironment(
                restAddress.getHost(), restAddress.getPort(), configuration);
        environment.setParallelism(1);
        environment.enableCheckpointing(Duration.ofMillis(100).toMillis());
        return environment;
    }

    private static String flinkTempDirectory() {
        return System.getProperty("flink.test.flink-tmp-dir");
    }

    private static String rocksDbDirectory() {
        return System.getProperty("flink.test.rocksdb-local-dir");
    }

    private static void closeMiniCluster(MiniCluster cluster) throws Exception {
        try {
            cluster.close();
        } catch (FlinkException exception) {
            if (!Boolean.getBoolean("flink.test.ignore-minicluster-directory-cleanup-failure")
                    || !hasCause(exception, java.nio.file.DirectoryNotEmptyException.class)) {
                throw exception;
            }
        }
    }

    private static boolean hasCause(Throwable exception, Class<? extends Throwable> type) {
        for (Throwable current = exception; current != null; current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }

    private static final class WriteState extends KeyedProcessFunction<Integer, Integer, String> {
        private final ValueStateDescriptor<GenericRecord> descriptor;
        private transient ValueState<GenericRecord> state;

        private WriteState(ValueStateDescriptor<GenericRecord> descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext context) throws Exception {
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Integer value, Context context, Collector<String> out) throws Exception {
            GenericRecord record = new GenericData.Record(V1);
            record.put("name", "Ada");
            state.update(record);
            out.collect("written");
        }
    }

    private static String awaitEvent(Path events) throws Exception {
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(1, Files.readAllLines(events).size()));
        return Files.readAllLines(events).get(0);
    }

    private static final class ReadState extends KeyedProcessFunction<Integer, Integer, String> {
        private final SchemaResolvingAvroSerializer<GenericRecord> serializer;
        private transient ValueState<GenericRecord> state;

        private ReadState(SchemaResolvingAvroSerializer<GenericRecord> serializer) {
            this.serializer = serializer;
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext context) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("customer", serializer));
        }

        @Override
        public void processElement(Integer value, Context context, Collector<String> out) throws Exception {
            GenericRecord state = this.state.value();
            out.collect(state.get("name").toString() + "|" + state.get("tier").toString());
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
}
