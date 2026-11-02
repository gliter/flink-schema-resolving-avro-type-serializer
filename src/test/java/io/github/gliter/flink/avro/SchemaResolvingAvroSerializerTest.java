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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
    private static final Schema V2_FIELD_INSERTED_FIRST = new Schema.Parser().parse("""
            {"type":"record","name":"Customer","namespace":"example",
             "fields":[{"name":"tier","type":"string","default":"standard"},
                       {"name":"name","type":"string"}]}
            """);
    private static final Schema INCOMPATIBLE_READER = new Schema.Parser().parse("""
            {"type":"record","name":"Customer","namespace":"example",
             "fields":[{"name":"name","type":"int"}]}
            """);
    private static final Schema TWO_BOOLEANS = new Schema.Parser().parse("""
            {"type":"record","name":"Flags","namespace":"example",
             "fields":[{"name":"a","type":"boolean"},{"name":"b","type":"boolean"}]}
            """);
    private static final Schema NUMERIC_FIRST = new Schema.Parser().parse("""
            {"type":"record","name":"Reading","namespace":"example",
             "fields":[{"name":"timestamp","type":"long"},{"name":"value","type":"double"}]}
            """);
    private static final Schema SHORT_NUMERIC_FIRST = new Schema.Parser().parse("""
            {"type":"record","name":"Counter","namespace":"example",
             "fields":[{"name":"count","type":"int"}]}
            """);

    @Test
    void fullRewriteResetsSchemaHistoryAndLazyRestoreKeepsIt() {
        Schema first = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"History\",\"fields\":[{\"name\":\"x\",\"type\":\"string\"}]}");
        Schema second = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"History\",\"fields\":[]}");
        Schema third = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"History\",\"fields\":[{\"name\":\"x\",\"type\":\"int\",\"default\":0}]}");
        var config = Map.<String, Object>of("schema.registry.url", "mock://history-reset");
        var one = SchemaResolvingAvroSerializer.forGeneric("history", first, config);
        var two = SchemaResolvingAvroSerializer.forGeneric("history", second, config);
        two.snapshotConfiguration().resolveSchemaCompatibility(one.snapshotConfiguration());
        var mixed = two.snapshotConfiguration();
        assertEquals(2, two.recordedSchemas().size());
        var three = SchemaResolvingAvroSerializer.forGeneric("history", third, config);
        assertTrue(three.snapshotConfiguration().resolveSchemaCompatibility(mixed).isIncompatible());
        var rewrite = SchemaResolvingAvroSerializer.forGeneric("history", second,
                Map.of("schema.registry.url", "mock://history-reset", "force.migration.nonce", 1,
                        "allow.full.migration", true));
        assertTrue(rewrite.snapshotConfiguration().resolveSchemaCompatibility(mixed).isCompatibleAfterMigration());
        var rewritten = rewrite.snapshotConfiguration();
        assertEquals(1, rewrite.recordedSchemas().size());
        assertTrue(three.snapshotConfiguration().resolveSchemaCompatibility(rewritten).isCompatibleAsIs());
    }

    @Test
    void fullMigrationRequiresExplicitPermission() {
        var previous = SchemaResolvingAvroSerializer.forGeneric("migration-gate", V1,
                Map.of("schema.registry.url", "mock://migration-gate")).snapshotConfiguration();
        for (Map<String, Object> request : java.util.List.of(
                Map.<String, Object>of("force.migration.nonce", 1),
                Map.<String, Object>of("frame.magic", "AB"))) {
            var config = new java.util.HashMap<String, Object>(request);
            config.put("schema.registry.url", "mock://migration-gate");
            var denied = SchemaResolvingAvroSerializer.forGeneric("migration-gate", V1, config);
            assertTrue(denied.snapshotConfiguration().resolveSchemaCompatibility(previous).isIncompatible());
            config.put("allow.full.migration", true);
            var allowed = SchemaResolvingAvroSerializer.forGeneric("migration-gate", V1, config);
            assertTrue(allowed.snapshotConfiguration().resolveSchemaCompatibility(previous).isCompatibleAfterMigration());
        }
        var legacy = new AvroSerializer<>(GenericRecord.class, NUMERIC_FIRST).snapshotConfiguration();
        var deniedLegacy = SchemaResolvingAvroSerializer.forGeneric("migration-gate", NUMERIC_FIRST,
                Map.of("schema.registry.url", "mock://migration-gate-legacy"));
        assertTrue(deniedLegacy.snapshotConfiguration().resolveSchemaCompatibility(legacy).isIncompatible());
    }

    @Test
    void unsafeLegacyMagicRequiresExplicitPolicy() {
        var legacy = new AvroSerializer<>(GenericRecord.class, NUMERIC_FIRST).snapshotConfiguration();
        var defaults = SchemaResolvingAvroSerializer.forGeneric("policy", NUMERIC_FIRST,
                Map.of("schema.registry.url", "mock://unsafe-policy"));
        assertTrue(defaults.snapshotConfiguration().resolveSchemaCompatibility(legacy).isIncompatible());
        var unsafe = SchemaResolvingAvroSerializer.forGeneric("policy", NUMERIC_FIRST,
                Map.of("schema.registry.url", "mock://unsafe-policy", "use.unsafe.magic", true));
        assertTrue(unsafe.snapshotConfiguration().resolveSchemaCompatibility(legacy)
                .isCompatibleWithReconfiguredSerializer());
        var migrate = SchemaResolvingAvroSerializer.forGeneric("policy", NUMERIC_FIRST,
                Map.of("schema.registry.url", "mock://unsafe-policy", "allow.full.migration", true));
        assertTrue(migrate.snapshotConfiguration().resolveSchemaCompatibility(legacy).isCompatibleAfterMigration());
    }

    @Test
    void newStateStartsWithVersionWithoutMagic() throws Exception {
        var serializer = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", V1, Map.of("schema.registry.url", "mock://empty-magic"));
        GenericRecord record = new GenericData.Record(V1);
        record.put("name", "Ada");
        DataOutputSerializer output = new DataOutputSerializer(64);
        serializer.serialize(record, output);
        assertEquals(0, serializer.frameMagic().length);
        assertEquals((byte) 1, output.getCopyOfBuffer()[0]);
        assertEquals(record, serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer())));
    }

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
                "force.migration.nonce", 1, "allow.full.migration", true);
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
                "force.migration.nonce", 2, "allow.full.migration", true);
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
    void forceMigrationNonceMustFitAnIntFromEitherConfigSource() {
        assertEquals(1, SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", V2, Map.of("force.migration.nonce", "1")).forceMigrationNonce());
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
                .forGeneric("customer-state", V2, config)
                .reconfigured(V1, new byte[] { (byte) 0xff });
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
    void readsLegacyValuesShorterThanTheFrameAndConsecutiveLegacyValues() throws Exception {
        // Two booleans encode to exactly two bytes under Flink's Avro serializer.
        GenericRecord flags = new GenericData.Record(TWO_BOOLEANS);
        flags.put("a", true);
        flags.put("b", false);
        byte[] one = legacyBytes(TWO_BOOLEANS, flags);
        assertEquals(2, one.length);
        SchemaResolvingAvroSerializer<GenericRecord> serializer =
                bridgedFromFlinkAvro(TWO_BOOLEANS, TWO_BOOLEANS, "mock://short-legacy");
        assertArrayEquals(new byte[] { (byte) 0xff }, serializer.frameMagic());

        assertEquals(true, serializer.deserialize(new DataInputDeserializer(one)).get("a"));

        // ListState and heap snapshots place values back to back in one stream.
        DataInputDeserializer input = new DataInputDeserializer(concat(one, one));
        serializer.deserialize(input);
        assertEquals(2, input.available());
        serializer.deserialize(input);
        assertEquals(0, input.available());
    }

    @Test
    void copyOfLegacyValueKeepsFieldValuesWhenReaderInsertsAField() throws Exception {
        GenericRecord record = new GenericData.Record(V1);
        record.put("name", "legacy");
        byte[] legacy = legacyBytes(V1, record);
        SchemaResolvingAvroSerializer<GenericRecord> serializer =
                bridgedFromFlinkAvro(V1, V2_FIELD_INSERTED_FIRST, "mock://copy-legacy");

        DataOutputSerializer copied = new DataOutputSerializer(64);
        serializer.copy(new DataInputDeserializer(legacy), copied);

        assertEquals("legacy|standard", readNameAndTier(
                serializer.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer()))));
    }

    @Test
    void numericFirstLegacySchemaUsesTheLongMagicOrMigratesWhenValuesCanBeShort() throws Exception {
        GenericRecord reading = new GenericData.Record(NUMERIC_FIRST);
        reading.put("timestamp", -42L);
        reading.put("value", 2.5d);
        SchemaResolvingAvroSerializer<GenericRecord> serializer =
                bridgedFromFlinkAvro(NUMERIC_FIRST, NUMERIC_FIRST, "mock://numeric-legacy");
        assertArrayEquals(SchemaResolvingAvroSerializer.HEURISTIC_FRAME_MAGIC, serializer.frameMagic());
        assertEquals(-42L, serializer.deserialize(new DataInputDeserializer(legacyBytes(NUMERIC_FIRST, reading)))
                .get("timestamp"));

        SchemaResolvingAvroSerializer<GenericRecord> counter = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", SHORT_NUMERIC_FIRST,
                Map.of("schema.registry.url", "mock://short-numeric-legacy", "auto.register.schemas", true,
                        "allow.full.migration", true));
        assertTrue(counter.snapshotConfiguration().resolveSchemaCompatibility(
                new AvroSerializer<>(GenericRecord.class, SHORT_NUMERIC_FIRST).snapshotConfiguration())
                .isCompatibleAfterMigration());
    }

    @Test
    void explicitFrameMagicIsHonouredAndChangingItForcesMigration() throws Exception {
        Map<String, Object> explicit = Map.of(
                "schema.registry.url", "mock://explicit-magic", "auto.register.schemas", true, "frame.magic", "AB");
        SchemaResolvingAvroSerializer<GenericRecord> writer = SchemaResolvingAvroSerializer.forGeneric("customer-state", V1, explicit);
        GenericRecord record = new GenericData.Record(V1);
        record.put("name", "Ada");
        DataOutputSerializer output = new DataOutputSerializer(64);
        writer.serialize(record, output);
        assertEquals((byte) 0xab, output.getCopyOfBuffer()[0]);
        SchemaResolvingAvroSerializerSnapshot<GenericRecord> snapshot =
                (SchemaResolvingAvroSerializerSnapshot<GenericRecord>) writer.snapshotConfiguration();

        SchemaResolvingAvroSerializer<GenericRecord> changedMagic = SchemaResolvingAvroSerializer.forGeneric("customer-state", V1, Map.of(
                "schema.registry.url", "mock://explicit-magic", "auto.register.schemas", true, "frame.magic", "CD", "allow.full.migration", true));
        assertTrue(changedMagic.snapshotConfiguration().resolveSchemaCompatibility(snapshot).isCompatibleAfterMigration());

        SchemaResolvingAvroSerializer<GenericRecord> unconfigured = SchemaResolvingAvroSerializer.forGeneric("customer-state", V1,
                Map.of("schema.registry.url", "mock://explicit-magic", "auto.register.schemas", true));
        TypeSerializerSchemaCompatibility<GenericRecord> carried = unconfigured.snapshotConfiguration().resolveSchemaCompatibility(snapshot);
        assertTrue(carried.isCompatibleWithReconfiguredSerializer());
        assertEquals("Ada", carried.getReconfiguredSerializer()
                .deserialize(new DataInputDeserializer(output.getCopyOfBuffer())).get("name").toString());

        assertThrows(IllegalArgumentException.class, () -> SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", V1, Map.of("frame.magic", "not-hex")));
    }

    @Test
    void rejectsReaderSchemaThatCannotReadKnownWriterSchemasUnlessVerificationIsDisabled() throws Exception {
        Map<String, Object> config = Map.of("schema.registry.url", "mock://verify", "auto.register.schemas", true);
        SchemaResolvingAvroSerializer<GenericRecord> v1 = SchemaResolvingAvroSerializer.forGeneric("customer-state", V1, config);
        GenericRecord record = new GenericData.Record(V1);
        record.put("name", "Ada");
        v1.serialize(record, new DataOutputSerializer(64));

        SchemaResolvingAvroSerializer<GenericRecord> incompatibleReader =
                SchemaResolvingAvroSerializer.forGeneric("customer-state", INCOMPATIBLE_READER, config);
        assertTrue(incompatibleReader.snapshotConfiguration().resolveSchemaCompatibility(v1.snapshotConfiguration())
                .isIncompatible());
        assertTrue(incompatibleReader.snapshotConfiguration().resolveSchemaCompatibility(
                new AvroSerializer<>(GenericRecord.class, V1).snapshotConfiguration()).isIncompatible());

        SchemaResolvingAvroSerializer<GenericRecord> unverified = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", INCOMPATIBLE_READER, Map.of("schema.registry.url", "mock://verify",
                        "auto.register.schemas", true, "verify.schema.compatibility", false));
        assertTrue(unverified.snapshotConfiguration().resolveSchemaCompatibility(v1.snapshotConfiguration())
                .isCompatibleAsIs());
    }

    @Test
    void copiesRecordsAndRejectsNullLikeFlinkAvroSerializer() {
        Map<String, Object> schemaRegistryConfig = Map.of("schema.registry.url", "mock://copy", "auto.register.schemas", true);
        SchemaResolvingAvroSerializer<GenericRecord> serializer = SchemaResolvingAvroSerializer.forGeneric("customer-state", V1, schemaRegistryConfig);
        GenericRecord original = new GenericData.Record(V1);
        original.put("name", "Lin");
        assertEquals(original, serializer.copy(original));

        assertThrows(NullPointerException.class, () -> serializer.serialize(null, new DataOutputSerializer(16)));
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
        assertThrows(IllegalStateException.class, () -> changedRegistry.snapshotConfiguration()
                .resolveSchemaCompatibility(oldSerializer.snapshotConfiguration()));
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
        input.skipBytesToRead(SchemaResolvingAvroSerializer.DEFAULT_FRAME_MAGIC.length + 1);
        return SchemaResolvingAvroSerializer.readVarInt(input);
    }

    private static byte[] legacyBytes(Schema schema, GenericRecord record) throws java.io.IOException {
        DataOutputSerializer output = new DataOutputSerializer(64);
        new AvroSerializer<>(GenericRecord.class, schema).serialize(record, output);
        return output.getCopyOfBuffer();
    }

    @SuppressWarnings("unchecked")
    private static SchemaResolvingAvroSerializer<GenericRecord> bridgedFromFlinkAvro(
            Schema legacySchema, Schema readerSchema, String registryUrl) {
        SchemaResolvingAvroSerializer<GenericRecord> current = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state", readerSchema, Map.of("schema.registry.url", registryUrl, "auto.register.schemas", true, "use.unsafe.magic", true));
        TypeSerializerSchemaCompatibility<GenericRecord> compatibility = current.snapshotConfiguration()
                .resolveSchemaCompatibility(new AvroSerializer<>(GenericRecord.class, legacySchema).snapshotConfiguration());
        assertTrue(compatibility.isCompatibleWithReconfiguredSerializer());
        return (SchemaResolvingAvroSerializer<GenericRecord>) compatibility.getReconfiguredSerializer();
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] both = java.util.Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, both, first.length, second.length);
        return both;
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
