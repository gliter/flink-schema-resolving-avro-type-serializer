package io.github.gliter.flink.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComplexSchemaEvolutionTest {
    private static final Schema V1 = new Schema.Parser().parse("""
            {"type":"record","name":"Customer","namespace":"complex",
             "fields":[
               {"name":"id","type":"string"},
               {"name":"profile","type":{"type":"record","name":"Profile","fields":[
                 {"name":"name","type":"string"},
                 {"name":"legacyCode","type":["null","string"],"default":null}]}},
               {"name":"addresses","type":{"type":"array","items":{"type":"record","name":"Address","fields":[
                 {"name":"street","type":"string"}]} }},
               {"name":"attributes","type":{"type":"map","values":"string"}},
               {"name":"tags","type":{"type":"array","items":"string"}},
               {"name":"legacyFlag","type":"boolean","default":false}
             ]}
            """);

    private static final Schema V2 = new Schema.Parser().parse("""
            {"type":"record","name":"Customer","namespace":"complex",
             "fields":[
               {"name":"id","type":"string"},
               {"name":"profile","type":{"type":"record","name":"Profile","fields":[
                 {"name":"name","type":"string"},
                 {"name":"tier","type":"string","default":"standard"}]}},
               {"name":"addresses","type":{"type":"array","items":{"type":"record","name":"Address","fields":[
                 {"name":"street","type":"string"},
                 {"name":"country","type":"string","default":"US"}]} }},
               {"name":"attributes","type":{"type":"map","values":"string"}},
               {"name":"tags","type":{"type":"array","items":"string"}},
               {"name":"labels","type":{"type":"map","values":"string"},"default":{}},
               {"name":"aliases","type":{"type":"array","items":"string"},"default":[]}
             ]}
            """);

    @Test
    void lazilyProjectsNestedGenericRecordWithMapsAndArrays() throws Exception {
        Map<String, Object> config = config("mock://complex-generic-lazy", false);
        SchemaResolvingAvroSerializer<GenericRecord> writer =
                SchemaResolvingAvroSerializer.forGeneric("customer-state", V1, config);
        SchemaResolvingAvroSerializer<GenericRecord> reader =
                SchemaResolvingAvroSerializer.forGeneric("customer-state", V2, config);

        GenericRecord restored = deserialize(reader, serialize(writer, genericV1Record()));

        assertComplexValues(restored);
        assertEquals(V2, restored.getSchema());
    }

    @Test
    void forceMigratesNestedGenericRecordToCurrentSchema() throws Exception {
        Map<String, Object> oldConfig = config("mock://complex-generic-force", false);
        Map<String, Object> migrationConfig = config("mock://complex-generic-force", true);
        SchemaResolvingAvroSerializer<GenericRecord> oldSerializer =
                SchemaResolvingAvroSerializer.forGeneric("customer-state", V1, oldConfig);
        SchemaResolvingAvroSerializer<GenericRecord> currentSerializer =
                SchemaResolvingAvroSerializer.forGeneric("customer-state", V2, migrationConfig);
        byte[] oldBytes = serialize(oldSerializer, genericV1Record());

        TypeSerializerSchemaCompatibility<GenericRecord> compatibility = currentSerializer.snapshotConfiguration()
                .resolveSchemaCompatibility(oldSerializer.snapshotConfiguration());
        assertTrue(compatibility.isCompatibleAfterMigration());

        GenericRecord historicalRecord = deserialize(oldSerializer, oldBytes);
        byte[] migratedBytes = serialize(currentSerializer, historicalRecord);

        assertEquals(V2, schemaForId("mock://complex-generic-force", schemaId(migratedBytes)));
        assertComplexValues(deserialize(currentSerializer, migratedBytes));
    }

    @Test
    void lazilyProjectsNestedSpecificRecordWithMapsAndArrays() throws Exception {
        try (GeneratedRecords v1 = generatedRecords(V1); GeneratedRecords v2 = generatedRecords(V2)) {
            Map<String, Object> config = config("mock://complex-specific-lazy", false);
            SchemaResolvingAvroSerializer<SpecificRecord> writer =
                    specificSerializer(v1, config);
            SchemaResolvingAvroSerializer<SpecificRecord> reader =
                    specificSerializer(v2, config);

            byte[] oldBytes = serialize(writer, specificV1Record(v1.loader()));
            SpecificRecord restored = withContextClassLoader(v2.loader(), () -> deserialize(reader, oldBytes));

            assertComplexValues(restored);
            assertEquals(V2, restored.getSchema());
        }
    }

    @Test
    void forceMigratesNestedSpecificRecordToCurrentSchema() throws Exception {
        try (GeneratedRecords v1 = generatedRecords(V1); GeneratedRecords v2 = generatedRecords(V2)) {
            Map<String, Object> oldConfig = config("mock://complex-specific-force", false);
            Map<String, Object> migrationConfig = config("mock://complex-specific-force", true);
            SchemaResolvingAvroSerializer<SpecificRecord> oldSerializer = specificSerializer(v1, oldConfig);
            SchemaResolvingAvroSerializer<SpecificRecord> currentSerializer = specificSerializer(v2, migrationConfig);
            byte[] oldBytes = serialize(oldSerializer, specificV1Record(v1.loader()));

            TypeSerializerSchemaCompatibility<SpecificRecord> compatibility = currentSerializer.snapshotConfiguration()
                    .resolveSchemaCompatibility(oldSerializer.snapshotConfiguration());
            assertTrue(compatibility.isCompatibleAfterMigration());

            SpecificRecord currentRecord = withContextClassLoader(v2.loader(),
                    () -> deserialize(currentSerializer, oldBytes));
            byte[] migratedBytes = withContextClassLoader(v2.loader(),
                    () -> serialize(currentSerializer, currentRecord));

            assertEquals(V2, schemaForId("mock://complex-specific-force", schemaId(migratedBytes)));
            assertComplexValues(withContextClassLoader(v2.loader(),
                    () -> deserialize(currentSerializer, migratedBytes)));
        }
    }

    private static Map<String, Object> config(String registryUrl, boolean forceMigration) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("schema.registry.url", registryUrl);
        config.put("auto.register.schemas", true);
        if (forceMigration) config.put("force.migration.nonce", 1);
        return Map.copyOf(config);
    }

    private static GenericRecord genericV1Record() {
        GenericRecord customer = new GenericData.Record(V1);
        customer.put("id", "customer-17");

        GenericRecord profile = new GenericData.Record(V1.getField("profile").schema());
        profile.put("name", "Ada Lovelace");
        profile.put("legacyCode", "old-code");
        customer.put("profile", profile);

        Schema addressSchema = V1.getField("addresses").schema().getElementType();
        GenericRecord firstAddress = new GenericData.Record(addressSchema);
        firstAddress.put("street", "12 Analytical Engine Way");
        GenericRecord secondAddress = new GenericData.Record(addressSchema);
        secondAddress.put("street", "4 Difference Lane");
        customer.put("addresses", List.of(firstAddress, secondAddress));

        customer.put("attributes", Map.of("segment", "research", "region", "eu"));
        customer.put("tags", List.of("founder", "mathematician"));
        customer.put("legacyFlag", true);
        return customer;
    }

    private static SpecificRecord specificV1Record(ClassLoader loader) throws Exception {
        SpecificRecord customer = newSpecificRecord("complex.Customer", loader);
        customer.put(0, "customer-17");

        SpecificRecord profile = newSpecificRecord("complex.Profile", loader);
        profile.put(0, "Ada Lovelace");
        profile.put(1, "old-code");
        customer.put(1, profile);

        SpecificRecord firstAddress = newSpecificRecord("complex.Address", loader);
        firstAddress.put(0, "12 Analytical Engine Way");
        SpecificRecord secondAddress = newSpecificRecord("complex.Address", loader);
        secondAddress.put(0, "4 Difference Lane");
        customer.put(2, List.of(firstAddress, secondAddress));

        customer.put(3, Map.of("segment", "research", "region", "eu"));
        customer.put(4, List.of("founder", "mathematician"));
        customer.put(5, true);
        return customer;
    }

    private static SpecificRecord newSpecificRecord(String className, ClassLoader loader) throws Exception {
        return (SpecificRecord) Class.forName(className, true, loader).getDeclaredConstructor().newInstance();
    }

    private static void assertComplexValues(GenericRecord customer) {
        assertEquals("customer-17", customer.get("id").toString());
        GenericRecord profile = (GenericRecord) customer.get("profile");
        assertEquals("Ada Lovelace", profile.get("name").toString());
        assertEquals("standard", profile.get("tier").toString());

        List<?> addresses = (List<?>) customer.get("addresses");
        assertEquals(2, addresses.size());
        assertEquals("12 Analytical Engine Way", ((GenericRecord) addresses.get(0)).get("street").toString());
        assertEquals("US", ((GenericRecord) addresses.get(0)).get("country").toString());
        assertEquals("4 Difference Lane", ((GenericRecord) addresses.get(1)).get("street").toString());
        assertEquals("US", ((GenericRecord) addresses.get(1)).get("country").toString());

        assertAttributes(customer.get("attributes"));
        assertEquals(List.of("founder", "mathematician"), stringValues(customer.get("tags")));
        assertEquals(Map.of(), customer.get("labels"));
        assertEquals(List.of(), stringValues(customer.get("aliases")));
    }

    private static void assertComplexValues(SpecificRecord customer) {
        assertEquals("customer-17", customer.get(0).toString());
        SpecificRecord profile = (SpecificRecord) customer.get(1);
        assertEquals("Ada Lovelace", profile.get(0).toString());
        assertEquals("standard", profile.get(1).toString());

        List<?> addresses = (List<?>) customer.get(2);
        assertEquals(2, addresses.size());
        assertEquals("12 Analytical Engine Way", ((SpecificRecord) addresses.get(0)).get(0).toString());
        assertEquals("US", ((SpecificRecord) addresses.get(0)).get(1).toString());
        assertEquals("4 Difference Lane", ((SpecificRecord) addresses.get(1)).get(0).toString());
        assertEquals("US", ((SpecificRecord) addresses.get(1)).get(1).toString());

        assertAttributes(customer.get(3));
        assertEquals(List.of("founder", "mathematician"), stringValues(customer.get(4)));
        assertEquals(Map.of(), customer.get(5));
        assertEquals(List.of(), stringValues(customer.get(6)));
    }

    private static void assertAttributes(Object value) {
        Map<?, ?> attributes = (Map<?, ?>) value;
        Map<String, String> normalized = new HashMap<>();
        attributes.forEach((key, entryValue) -> normalized.put(key.toString(), entryValue.toString()));
        assertEquals(Map.of("segment", "research", "region", "eu"), normalized);
    }

    private static List<String> stringValues(Object value) {
        return ((List<?>) value).stream().map(Object::toString).toList();
    }

    private static SchemaResolvingAvroSerializer<SpecificRecord> specificSerializer(
            GeneratedRecords records, Map<String, Object> config) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        SchemaResolvingAvroSerializer<SpecificRecord> serializer = (SchemaResolvingAvroSerializer)
                SchemaResolvingAvroSerializer.forSpecific("customer-state", (Class) records.customerClass(), config);
        return serializer;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static byte[] serialize(SchemaResolvingAvroSerializer<?> serializer, Object value) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(512);
        SchemaResolvingAvroSerializer rawSerializer = serializer;
        rawSerializer.serialize((IndexedRecord) value, output);
        return output.getCopyOfBuffer();
    }

    private static <T extends IndexedRecord> T deserialize(
            SchemaResolvingAvroSerializer<T> serializer, byte[] bytes) throws Exception {
        return serializer.deserialize(new DataInputDeserializer(bytes));
    }

    private static <T> T withContextClassLoader(ClassLoader loader, ThrowingSupplier<T> operation) throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(loader);
            return operation.get();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    private static Schema schemaForId(String registryUrl, int id) throws Exception {
        try (SchemaRegistryClient client = SchemaRegistryClientFactory.newClient(registryUrl, 1000,
                List.of(new AvroSchemaProvider()), Map.of("schema.registry.url", registryUrl), Map.of())) {
            return ((AvroSchema) client.getSchemaById(id)).rawSchema();
        }
    }

    private static int schemaId(byte[] bytes) throws Exception {
        DataInputDeserializer input = new DataInputDeserializer(bytes);
        input.skipBytesToRead(SchemaResolvingAvroSerializer.WIRE_MAGIC.length + 1 + 1 + Integer.BYTES + 1);
        return input.readInt();
    }

    private static GeneratedRecords generatedRecords(Schema schema) throws Exception {
        Path root = Files.createTempDirectory("complex-avro-specific");
        Path sourcesDirectory = root.resolve("sources");
        Path classesDirectory = root.resolve("classes");
        Files.createDirectories(sourcesDirectory);
        Files.createDirectories(classesDirectory);
        new SpecificCompiler(schema).compileToDestination(sourcesDirectory.toFile(), sourcesDirectory.toFile());

        List<String> sources;
        try (Stream<Path> paths = Files.walk(sourcesDirectory)) {
            sources = paths.filter(path -> path.toString().endsWith(".java"))
                    .map(Path::toString)
                    .toList();
        }
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) throw new IllegalStateException("A JDK is required to compile generated Avro records");
        List<String> arguments = new ArrayList<>(List.of(
                "-classpath", System.getProperty("java.class.path"), "-d", classesDirectory.toString()));
        arguments.addAll(sources);
        if (compiler.run(null, null, null, arguments.toArray(String[]::new)) != 0) {
            throw new IllegalStateException("Could not compile generated complex Avro records");
        }

        URLClassLoader loader = new URLClassLoader(new URL[] {classesDirectory.toUri().toURL()},
                ComplexSchemaEvolutionTest.class.getClassLoader());
        Class<?> customerClass = Class.forName("complex.Customer", true, loader);
        return new GeneratedRecords(loader, customerClass);
    }

    private record GeneratedRecords(URLClassLoader loader, Class<?> customerClass) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            loader.close();
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
