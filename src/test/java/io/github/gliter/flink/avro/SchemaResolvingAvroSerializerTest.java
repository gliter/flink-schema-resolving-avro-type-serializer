package io.github.gliter.flink.avro;

import io.github.gliter.flink.avro.model.Greeting;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.specific.SpecificRecord;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaResolvingAvroSerializerTest {
    private static final Schema V1 = new Schema.Parser().parse("""
            {"type":"record","name":"Customer","namespace":"example",
             "fields":[{"name":"name","type":"string"}]}
            """);
    private static final Schema V2 = new Schema.Parser().parse("""
            {"type":"record","name":"Customer","namespace":"example",
             "fields":[{"name":"name","type":"string"},
                       {"name":"tier","type":"string","default":"standard"}]}
            """);
    private static final Schema SPECIFIC_V1 = new Schema.Parser().parse("""
            {"type":"record","name":"Customer","namespace":"state",
             "fields":[{"name":"name","type":"string"}]}
            """);
    private static final Schema SPECIFIC_V2 = new Schema.Parser().parse("""
            {"type":"record","name":"Customer","namespace":"state",
             "fields":[{"name":"name","type":"string"},
                       {"name":"tier","type":"string","default":"standard"}]}
            """);

    @Test
    void recordKindSnapshotIdsAreExplicitAndRejectUnknownValues() {
        assertEquals(1, AvroRecordKind.SPECIFIC.snapshotId());
        assertEquals(2, AvroRecordKind.GENERIC.snapshotId());
        assertEquals(AvroRecordKind.SPECIFIC, AvroRecordKind.fromSnapshotId(1));
        assertEquals(AvroRecordKind.GENERIC, AvroRecordKind.fromSnapshotId(2));
        assertThrows(IllegalArgumentException.class, () -> AvroRecordKind.fromSnapshotId(0));
    }

    @Test
    void resolvesOldConfluentBytesWithNewReaderSchemaWithoutMigration() throws Exception {
        Map<String, Object> schemaRegistryConfig = Map.of("schema.registry.url", "mock://evolution", "auto.register.schemas", true);
        SchemaResolvingAvroSerializer<GenericRecord> v1 = SchemaResolvingAvroSerializer.forGeneric("customer-state", V1, schemaRegistryConfig);
        SchemaResolvingAvroSerializer<GenericRecord> v2 = SchemaResolvingAvroSerializer.forGeneric("customer-state", V2, schemaRegistryConfig);
        GenericRecord oldRecord = new GenericData.Record(V1);
        oldRecord.put("name", "Ada");

        DataOutputSerializer output = new DataOutputSerializer(128);
        v1.serialize(oldRecord, output);
        GenericRecord reuse = new GenericData.Record(V2);
        GenericRecord restored = v2.deserialize(reuse, new DataInputDeserializer(output.getCopyOfBuffer()));

        assertSame(reuse, restored);
        assertEquals("Ada", restored.get("name").toString());
        assertEquals("standard", restored.get("tier").toString());
        TypeSerializerSchemaCompatibility<GenericRecord> compatibility = v2.snapshotConfiguration()
                .resolveSchemaCompatibility(v1.snapshotConfiguration());
        assertTrue(compatibility.isCompatibleAsIs());
    }

    @Test
    void acceptsReaderSchemaChangesWithoutStateRewrite() {
        Map<String, Object> schemaRegistryConfig = Map.of("schema.registry.url", "mock://strict", "auto.register.schemas", true);
        SchemaResolvingAvroSerializer<GenericRecord> v1 = SchemaResolvingAvroSerializer.forGeneric("customer-state", V1, schemaRegistryConfig);
        SchemaResolvingAvroSerializer<GenericRecord> v2 = SchemaResolvingAvroSerializer.forGeneric("customer-state", V2, schemaRegistryConfig);

        assertTrue(v2.snapshotConfiguration().resolveSchemaCompatibility(v1.snapshotConfiguration()).isCompatibleAsIs());
    }

    @Test
    void forceMigrationNonceRewritesOnceAndChangesTriggerAnotherMigration() throws Exception {
        Map<String, Object> oldConfig = Map.of(
                "schema.registry.url", "mock://forced-migration",
                "auto.register.schemas", true);
        Map<String, Object> config = Map.of(
                "schema.registry.url", "mock://forced-migration",
                "auto.register.schemas", true,
                "force.migration.nonce", 1);
        SchemaResolvingAvroSerializer<GenericRecord> oldSerializer =
                SchemaResolvingAvroSerializer.forGeneric("customer-state", V1, oldConfig);
        SchemaResolvingAvroSerializer<GenericRecord> currentSerializer =
                SchemaResolvingAvroSerializer.forGeneric("customer-state", V2, config);

        GenericRecord oldRecord = new GenericData.Record(V1);
        oldRecord.put("name", "Ada");
        SchemaResolvingAvroSerializerSnapshot<GenericRecord> oldSnapshot =
                (SchemaResolvingAvroSerializerSnapshot<GenericRecord>) oldSerializer.snapshotConfiguration();
        @SuppressWarnings("unchecked")
        TypeSerializer<GenericRecord> migrationReader = (TypeSerializer<GenericRecord>) oldSnapshot.restoreSerializer();
        assertNull(migrationReader.createInstance());
        TypeSerializerSchemaCompatibility<GenericRecord> compatibility = currentSerializer.snapshotConfiguration()
                .resolveSchemaCompatibility(oldSnapshot);
        assertTrue(compatibility.isCompatibleAfterMigration());
        GenericRecord migrated = new GenericData.Record(V1);
        migrated.put("name", "Ada");
        assertThrows(UnsupportedOperationException.class,
                () -> migrationReader.serialize(migrated, new DataOutputSerializer(128)));

        DataOutputSerializer rewritten = new DataOutputSerializer(128);
        currentSerializer.serialize(migrated, rewritten);
        assertEquals(V2, schemaFromId("mock://forced-migration", schemaIdFromFramedValue(rewritten.getCopyOfBuffer())));

        SchemaResolvingAvroSerializerSnapshot<GenericRecord> migratedSnapshot =
                (SchemaResolvingAvroSerializerSnapshot<GenericRecord>) currentSerializer.snapshotConfiguration();
        DataOutputSerializer snapshotOutput = new DataOutputSerializer(128);
        migratedSnapshot.writeSnapshot(snapshotOutput);
        SchemaResolvingAvroSerializerSnapshot<GenericRecord> restoredSnapshot =
                new SchemaResolvingAvroSerializerSnapshot<>();
        assertEquals(1, migratedSnapshot.getCurrentVersion());
        restoredSnapshot.readSnapshot(1, new DataInputDeserializer(snapshotOutput.getCopyOfBuffer()),
                getClass().getClassLoader());

        SchemaResolvingAvroSerializer<GenericRecord> sameNonceSerializer =
                SchemaResolvingAvroSerializer.forGeneric("customer-state", V2, config);
        assertTrue(sameNonceSerializer.snapshotConfiguration().resolveSchemaCompatibility(restoredSnapshot)
                .isCompatibleAsIs());

        Map<String, Object> nextNonceConfig = Map.of(
                "schema.registry.url", "mock://forced-migration",
                "auto.register.schemas", true,
                "force.migration.nonce", 2);
        SchemaResolvingAvroSerializer<GenericRecord> nextNonceSerializer =
                SchemaResolvingAvroSerializer.forGeneric("customer-state", V2, nextNonceConfig);
        assertTrue(nextNonceSerializer.snapshotConfiguration().resolveSchemaCompatibility(restoredSnapshot)
                .isCompatibleAfterMigration());

        AvroSerializer<GenericRecord> oldAvro = new AvroSerializer<>(GenericRecord.class, V1);
        DataOutputSerializer oldAvroBytes = new DataOutputSerializer(64);
        oldAvro.serialize(oldRecord, oldAvroBytes);
        TypeSerializerSchemaCompatibility<GenericRecord> avroCompatibility = currentSerializer.snapshotConfiguration()
                .resolveSchemaCompatibility(oldAvro.snapshotConfiguration());
        assertTrue(avroCompatibility.isCompatibleAfterMigration());
        GenericRecord fromAvro = oldAvro.snapshotConfiguration().restoreSerializer()
                .deserialize(new DataInputDeserializer(oldAvroBytes.getCopyOfBuffer()));
        DataOutputSerializer rewrittenAvro = new DataOutputSerializer(128);
        currentSerializer.serialize(fromAvro, rewrittenAvro);
        assertEquals(V2, schemaFromId("mock://forced-migration",
                schemaIdFromFramedValue(rewrittenAvro.getCopyOfBuffer())));
    }

    @Test
    void forceMigrationNonceMustBeAnInt() {
        assertThrows(IllegalArgumentException.class, () -> SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", V2, Map.of("force.migration.nonce", "1")));
        assertThrows(IllegalArgumentException.class, () -> SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", V2, Map.of("force.migration.nonce", 2_147_483_648L)));
    }

    @Test
    void migrationNonceCanComeFromFlinkConfigAndSerializerMapTakesPriority() {
        Map<String, Object> flinkConfig = Map.of(
                "schema.resolving.avro.force.migration.nonce", "7");
        assertEquals(7, SchemaResolvingAvroSerializer.migrationNonceFromConfig(Map.of(), flinkConfig));
        assertEquals(3, SchemaResolvingAvroSerializer.migrationNonceFromConfig(
                Map.of("force.migration.nonce", 3), flinkConfig));
        assertThrows(IllegalArgumentException.class, () -> SchemaResolvingAvroSerializer.migrationNonceFromConfig(
                Map.of(), Map.of("schema.resolving.avro.force.migration.nonce", "not-a-number")));
    }

    @Test
    void restoredSnapshotSerializerIsAnEagerReadOnlySerializer() throws Exception {
        SchemaResolvingAvroSerializer<GenericRecord> serializer = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", V1, Map.of("schema.registry.url", "mock://snapshot", "auto.register.schemas", true));

        TypeSerializer<GenericRecord> restored = serializer.snapshotConfiguration().restoreSerializer();
        assertNull(restored.createInstance());
        GenericRecord record = new GenericData.Record(V1);
        assertThrows(UnsupportedOperationException.class,
                () -> restored.serialize(record, new DataOutputSerializer(128)));
    }

    @Test
    void readsRegularFlinkAvroStateAndReconfiguresWithoutMigration() throws Exception {
        AvroSerializer<GenericRecord> oldAvroSerializer = new AvroSerializer<>(GenericRecord.class, V1);
        GenericRecord oldRecord = new GenericData.Record(V1);
        oldRecord.put("name", "Ada");
        DataOutputSerializer oldBytes = new DataOutputSerializer(64);
        oldAvroSerializer.serialize(oldRecord, oldBytes);

        SchemaResolvingAvroSerializer<GenericRecord> current = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", V2,
                Map.of("schema.registry.url", "mock://legacy", "auto.register.schemas", true));
        TypeSerializerSchemaCompatibility<GenericRecord> compatibility = current.snapshotConfiguration()
                .resolveSchemaCompatibility(oldAvroSerializer.snapshotConfiguration());

        assertTrue(compatibility.isCompatibleWithReconfiguredSerializer());
        @SuppressWarnings("unchecked")
        SchemaResolvingAvroSerializer<GenericRecord> reconfigured =
                (SchemaResolvingAvroSerializer<GenericRecord>) compatibility.getReconfiguredSerializer();
        GenericRecord reuse = new GenericData.Record(V2);
        GenericRecord restored = reconfigured.deserialize(reuse, new DataInputDeserializer(oldBytes.getCopyOfBuffer()));
        assertSame(reuse, restored);
        assertEquals("Ada", restored.get("name").toString());
        assertEquals("standard", restored.get("tier").toString());
        TypeSerializerSchemaCompatibility<GenericRecord> nextCompatibility = current.snapshotConfiguration()
                .resolveSchemaCompatibility(reconfigured.snapshotConfiguration());
        assertTrue(nextCompatibility.isCompatibleWithReconfiguredSerializer());
        @SuppressWarnings("unchecked")
        SchemaResolvingAvroSerializer<GenericRecord> carried =
                (SchemaResolvingAvroSerializer<GenericRecord>) nextCompatibility.getReconfiguredSerializer();
        assertEquals("Ada", carried.deserialize(new DataInputDeserializer(oldBytes.getCopyOfBuffer()))
                .get("name").toString());
    }

    @Test
    void readsLegacyAndConfluentValuesSideBySideAndPersistsLegacySchema() throws Exception {
        Map<String, Object> config = Map.of("schema.registry.url", "mock://mixed", "auto.register.schemas", true);
        AvroSerializer<GenericRecord> oldAvroSerializer = new AvroSerializer<>(GenericRecord.class, V1);
        GenericRecord oldRecord = new GenericData.Record(V1);
        oldRecord.put("name", "legacy");
        DataOutputSerializer oldBytes = new DataOutputSerializer(64);
        oldAvroSerializer.serialize(oldRecord, oldBytes);

        SchemaResolvingAvroSerializer<GenericRecord> mixed = SchemaResolvingAvroSerializer
                .forGeneric("customer-state", V2, config).withLegacyWriterSchema(V1);
        GenericRecord newRecord = new GenericData.Record(V2);
        newRecord.put("name", "confluent");
        newRecord.put("tier", "gold");
        DataOutputSerializer newBytes = new DataOutputSerializer(128);
        mixed.serialize(newRecord, newBytes);

        byte[] both = new byte[oldBytes.length() + newBytes.length()];
        System.arraycopy(oldBytes.getCopyOfBuffer(), 0, both, 0, oldBytes.length());
        System.arraycopy(newBytes.getCopyOfBuffer(), 0, both, oldBytes.length(), newBytes.length());
        DataInputDeserializer input = new DataInputDeserializer(both);
        assertEquals("legacy|standard", readNameAndTier(mixed.deserialize(input)));
        assertEquals("confluent|gold", readNameAndTier(mixed.deserialize(input)));

        DataInputDeserializer legacyToCopy = new DataInputDeserializer(oldBytes.getCopyOfBuffer());
        DataOutputSerializer legacyCopy = new DataOutputSerializer(64);
        mixed.copy(legacyToCopy, legacyCopy);
        assertEquals("legacy|standard", readNameAndTier(
                mixed.deserialize(new DataInputDeserializer(legacyCopy.getCopyOfBuffer()))));

        SchemaResolvingAvroSerializerSnapshot<GenericRecord> snapshot =
                (SchemaResolvingAvroSerializerSnapshot<GenericRecord>) mixed.snapshotConfiguration();
        DataOutputSerializer snapshotBytes = new DataOutputSerializer(128);
        snapshot.writeSnapshot(snapshotBytes);
        SchemaResolvingAvroSerializerSnapshot<GenericRecord> restoredSnapshot =
                new SchemaResolvingAvroSerializerSnapshot<>();
        restoredSnapshot.readSnapshot(snapshot.getCurrentVersion(),
                new DataInputDeserializer(snapshotBytes.getCopyOfBuffer()), getClass().getClassLoader());
        assertNull(restoredSnapshot.restoreSerializer().createInstance());
        TypeSerializerSchemaCompatibility<GenericRecord> nextUpgrade =
                SchemaResolvingAvroSerializer.forGeneric("customer-state", V2, config).snapshotConfiguration()
                        .resolveSchemaCompatibility(restoredSnapshot);
        assertTrue(nextUpgrade.isCompatibleWithReconfiguredSerializer());
    }

    @Test
    void readsRegularFlinkAvroSpecificRecord() throws Exception {
        AvroSerializer<Greeting> oldAvroSerializer = new AvroSerializer<>(Greeting.class);
        Greeting oldRecord = new Greeting("hello from legacy");
        DataOutputSerializer oldBytes = new DataOutputSerializer(64);
        oldAvroSerializer.serialize(oldRecord, oldBytes);

        SchemaResolvingAvroSerializer<Greeting> current = SchemaResolvingAvroSerializer.forSpecific(
                "greeting-state", Greeting.class,
                Map.of("schema.registry.url", "mock://legacy-specific", "auto.register.schemas", true));
        TypeSerializerSchemaCompatibility<Greeting> compatibility = current.snapshotConfiguration()
                .resolveSchemaCompatibility(oldAvroSerializer.snapshotConfiguration());
        assertTrue(compatibility.isCompatibleWithReconfiguredSerializer());
        @SuppressWarnings("unchecked")
        SchemaResolvingAvroSerializer<Greeting> reconfigured =
                (SchemaResolvingAvroSerializer<Greeting>) compatibility.getReconfiguredSerializer();
        Greeting reuse = new Greeting("previous value");
        Greeting restored = reconfigured.deserialize(reuse, new DataInputDeserializer(oldBytes.getCopyOfBuffer()));
        assertSame(reuse, restored);
        assertEquals("hello from legacy", restored.message());
    }

    private static String readNameAndTier(GenericRecord record) {
        return record.get("name") + "|" + record.get("tier");
    }

    @Test
    void copiesAndRoundTripsNull() throws Exception {
        Map<String, Object> schemaRegistryConfig = Map.of("schema.registry.url", "mock://copy", "auto.register.schemas", true);
        SchemaResolvingAvroSerializer<GenericRecord> serializer = SchemaResolvingAvroSerializer.forGeneric("customer-state", V1, schemaRegistryConfig);
        GenericRecord original = new GenericData.Record(V1);
        original.put("name", "Lin");
        assertEquals(original, serializer.copy(original));

        DataOutputSerializer output = new DataOutputSerializer(16);
        serializer.serialize(null, output);
        assertNull(serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer())));
    }

    @Test
    void roundTripsGeneratedSpecificRecord() throws Exception {
        Map<String, Object> schemaRegistryConfig = Map.of("schema.registry.url", "mock://specific", "auto.register.schemas", true);
        SchemaResolvingAvroSerializer<Greeting> serializer = SchemaResolvingAvroSerializer.forSpecific(
                "greeting-state", Greeting.class, schemaRegistryConfig);
        DataOutputSerializer output = new DataOutputSerializer(64);
        serializer.serialize(new Greeting("hello"), output);

        Greeting reuse = new Greeting("previous value");
        Greeting restored = serializer.deserialize(reuse, new DataInputDeserializer(output.getCopyOfBuffer()));
        assertSame(reuse, restored);
        assertEquals("hello", restored.message());
    }

    @Test
    void resolvesConfluentBytesIntoNewGeneratedSpecificClass() throws Exception {
        try (URLClassLoader v1Loader = generatedClasses(SPECIFIC_V1);
             URLClassLoader v2Loader = generatedClasses(SPECIFIC_V2)) {
            Class<? extends SpecificRecord> v1Class = specificClass(v1Loader);
            Class<? extends SpecificRecord> v2Class = specificClass(v2Loader);
            Map<String, Object> config = Map.of(
                    "schema.registry.url", "mock://specific-evolution-test",
                    "auto.register.schemas", true);

            SchemaResolvingAvroSerializer<SpecificRecord> writer = specificSerializer(v1Class, config);
            SpecificRecord oldRecord = v1Class.getDeclaredConstructor().newInstance();
            oldRecord.put(0, "Ada");
            DataOutputSerializer bytes = new DataOutputSerializer(128);
            writer.serialize(oldRecord, bytes);

            SchemaResolvingAvroSerializer<SpecificRecord> reader = specificSerializer(v2Class, config);
            TypeSerializerSchemaCompatibility<SpecificRecord> compatibility = reader.snapshotConfiguration()
                    .resolveSchemaCompatibility(writer.snapshotConfiguration());
            SpecificRecord reuse = v2Class.getDeclaredConstructor().newInstance();
            SpecificRecord restored = withContextClassLoader(v2Loader,
                    () -> reader.deserialize(reuse, new DataInputDeserializer(bytes.getCopyOfBuffer())));

            assertTrue(compatibility.isCompatibleAsIs());
            assertSame(reuse, restored);
            assertSame(v2Class, restored.getClass());
            assertEquals("Ada", restored.get(0).toString());
            assertEquals("standard", restored.get(1).toString());
        }
    }

    @Test
    void acceptsChangedSubjectOrRegistryConfiguration() {
        SchemaResolvingAvroSerializer<GenericRecord> oldSerializer = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", V1, Map.of("schema.registry.url", "mock://one", "auto.register.schemas", true));
        SchemaResolvingAvroSerializer<GenericRecord> changedSubject = SchemaResolvingAvroSerializer.forGeneric(
                "other-state", V2, Map.of("schema.registry.url", "mock://one", "auto.register.schemas", true));
        SchemaResolvingAvroSerializer<GenericRecord> changedRegistry = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", V2, Map.of("schema.registry.url", "mock://two", "auto.register.schemas", true));

        assertTrue(changedSubject.snapshotConfiguration().resolveSchemaCompatibility(oldSerializer.snapshotConfiguration()).isCompatibleAsIs());
        assertTrue(changedRegistry.snapshotConfiguration().resolveSchemaCompatibility(oldSerializer.snapshotConfiguration()).isCompatibleAsIs());
    }

    @Test
    void registersUnderLiteralConfiguredSubject() throws Exception {
        String subject = "customer-state-exact";
        Map<String, Object> config = Map.of(
                "schema.registry.url", "mock://literal-subject",
                "auto.register.schemas", true);
        SchemaResolvingAvroSerializer<GenericRecord> serializer =
                SchemaResolvingAvroSerializer.forGeneric(subject, V1, config);
        GenericRecord record = new GenericData.Record(V1);
        record.put("name", "Ada");
        serializer.serialize(record, new DataOutputSerializer(128));

        try (SchemaRegistryClient client = registryClient(config)) {
            assertTrue(client.getAllSubjects().contains(subject));
            assertFalse(client.getAllSubjects().contains(subject + "-value"));
        }
    }

    @Test
    void autoRegisterDisabledRequiresSchemaUnderLiteralSubject() throws Exception {
        String subject = "customer-state-preregistered";
        Map<String, Object> config = Map.of(
                "schema.registry.url", "mock://preregistered",
                "auto.register.schemas", false);
        try (SchemaRegistryClient client = registryClient(config)) {
            client.register(subject, new AvroSchema(V1), false);
        }

        SchemaResolvingAvroSerializer<GenericRecord> serializer =
                SchemaResolvingAvroSerializer.forGeneric(subject, V1, config);
        GenericRecord record = new GenericData.Record(V1);
        record.put("name", "Ada");
        serializer.serialize(record, new DataOutputSerializer(128));

        Map<String, Object> missingSchemaConfig = Map.of(
                "schema.registry.url", "mock://missing-preregistered",
                "auto.register.schemas", false);
        SchemaResolvingAvroSerializer<GenericRecord> missingSchemaSerializer =
                SchemaResolvingAvroSerializer.forGeneric(subject, V1, missingSchemaConfig);
        assertThrows(java.io.IOException.class,
                () -> missingSchemaSerializer.serialize(record, new DataOutputSerializer(128)));
    }

    @Test
    void logsAndIgnoresKafkaSerdeOnlyConfiguration() {
        SchemaResolvingAvroSerializer<GenericRecord> serializer = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", V1, Map.of(
                        "schema.registry.url", "mock://unsupported-config",
                        "specific.avro.reader", true));
        assertFalse(serializer.serializerConfig().containsKey("specific.avro.reader"));
    }

    @Test
    void resolvesSecretReferencesFromEnvironmentOrFileWithoutBoth() throws Exception {
        Map<String, Object> serializerConfig = Map.of("schema.registry.url", "https://registry.example");
        Map<String, String> environmentConfig = Map.of(
                "schema.resolving.avro.basic.auth.credentials.source", "USER_INFO",
                "schema.resolving.avro.secret.schema.registry.basic.auth.user.info.env", "REGISTRY_USER_INFO");
        Map<String, Object> fromEnvironment = SchemaResolvingAvroSerializer.resolveClientConfig(
                serializerConfig, environmentConfig, Map.of("REGISTRY_USER_INFO", "alice:secret-value"));
        assertEquals("alice:secret-value", fromEnvironment.get("schema.registry.basic.auth.user.info"));

        java.nio.file.Path secretFile = Files.createTempFile("schema-registry-secret", ".txt");
        Files.writeString(secretFile, "  bob:another-secret  \r\n");
        Map<String, String> fileConfig = Map.of(
                "schema.resolving.avro.secret.schema.registry.basic.auth.user.info.file", secretFile.toString());
        Map<String, Object> fromFile = SchemaResolvingAvroSerializer.resolveClientConfig(
                serializerConfig, fileConfig, Map.of());
        assertEquals("bob:another-secret", fromFile.get("schema.registry.basic.auth.user.info"));

        Map<String, String> conflictingSources = Map.of(
                "schema.resolving.avro.secret.schema.registry.basic.auth.user.info.file", secretFile.toString(),
                "schema.resolving.avro.secret.schema.registry.basic.auth.user.info.env", "REGISTRY_USER_INFO");
        assertThrows(IllegalArgumentException.class, () -> SchemaResolvingAvroSerializer.resolveClientConfig(
                serializerConfig, conflictingSources, Map.of("REGISTRY_USER_INFO", "alice:secret-value")));

        assertThrows(IllegalStateException.class, () -> SchemaResolvingAvroSerializer.resolveClientConfig(
                serializerConfig, environmentConfig, Map.of()));
    }

    @Test
    void serializerConfigTakesPriorityOverFlinkGlobalConfig() {
        Map<String, Object> serializerConfig = Map.of("schema.registry.url", "mock://serializer-config");
        Map<String, String> flinkConfig = Map.of(
                "schema.resolving.avro.schema.registry.url", "mock://flink-config",
                "schema.resolving.avro.request.timeout.ms", "8000");

        Map<String, Object> resolved = SchemaResolvingAvroSerializer.resolveClientConfig(
                serializerConfig, flinkConfig, Map.of());
        assertEquals("mock://serializer-config", resolved.get("schema.registry.url"));
        assertEquals("8000", resolved.get("request.timeout.ms"));

        Map<String, Object> globalFallback = SchemaResolvingAvroSerializer.resolveClientConfig(
                Map.of(), flinkConfig, Map.of());
        assertEquals("mock://flink-config", globalFallback.get("schema.registry.url"));
    }

    private static int schemaIdFromFramedValue(byte[] bytes) throws java.io.IOException {
        DataInputDeserializer input = new DataInputDeserializer(bytes);
        input.skipBytesToRead(SchemaResolvingAvroSerializer.WIRE_MAGIC.length + 1 + 1 + Integer.BYTES + 1);
        return input.readInt();
    }

    private static Schema schemaFromId(String url, int schemaId) throws Exception {
        try (SchemaRegistryClient client = SchemaRegistryClientFactory.newClient(url, 1000,
                List.of(new AvroSchemaProvider()), Map.of("schema.registry.url", url), Map.of())) {
            return ((AvroSchema) client.getSchemaById(schemaId)).rawSchema();
        }
    }

    private static SchemaRegistryClient registryClient(Map<String, ?> config) {
        String url = config.get("schema.registry.url").toString();
        return SchemaRegistryClientFactory.newClient(url, 1000,
                List.of(new AvroSchemaProvider()), Map.of("schema.registry.url", url), Map.of());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static SchemaResolvingAvroSerializer<SpecificRecord> specificSerializer(
            Class<? extends SpecificRecord> recordClass, Map<String, Object> config) {
        return (SchemaResolvingAvroSerializer) SchemaResolvingAvroSerializer.forSpecific(
                "customer-state", (Class) recordClass, config);
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends SpecificRecord> specificClass(ClassLoader classLoader)
            throws ClassNotFoundException {
        return (Class<? extends SpecificRecord>) Class.forName("state.Customer", true, classLoader);
    }

    private static URLClassLoader generatedClasses(Schema schema) throws Exception {
        Path root = Files.createTempDirectory("generated-specific-record");
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
        if (compiler == null) {
            throw new IllegalStateException("A JDK is required to compile generated Avro test records");
        }
        List<String> arguments = new java.util.ArrayList<>(List.of(
                "-classpath", System.getProperty("java.class.path"), "-d", classesDirectory.toString()));
        arguments.addAll(sources);
        if (compiler.run(null, null, null, arguments.toArray(String[]::new)) != 0) {
            throw new IllegalStateException("Could not compile generated Avro test record");
        }
        return new URLClassLoader(new URL[] {classesDirectory.toUri().toURL()},
                SchemaResolvingAvroSerializerTest.class.getClassLoader());
    }

    private static <T> T withContextClassLoader(ClassLoader classLoader, ThrowingSupplier<T> supplier)
            throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(classLoader);
            return supplier.get();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
