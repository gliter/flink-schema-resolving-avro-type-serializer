package io.github.gliter.flink.avro;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.formats.avro.utils.DataInputDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A Flink state serializer that stores Avro bytes framed with the Schema Registry id of their writer schema.
 *
 * <p>Each value keeps its writer schema id, so the current reader schema resolves historical bytes without
 * rewriting otherwise stable state.</p>
 *
 * @param <T> the Avro record representation
 */
public final class SchemaResolvingAvroSerializer<T extends IndexedRecord> extends TypeSerializer<T> {
    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaResolvingAvroSerializer.class);
    /** Frame magic for state that never held raw Flink Avro values. */
    static final byte[] DEFAULT_FRAME_MAGIC = {};
    /** Longer frame magic for legacy writer schemas whose values may start with any byte. */
    static final byte[] HEURISTIC_FRAME_MAGIC = { 'S', 'R', 'A', 'V', 'R', 'O' };
    static final byte WIRE_VERSION = 1;
    static final String ALLOW_FULL_MIGRATION = "allow.full.migration";
    static final String USE_UNSAFE_MAGIC = "use.unsafe.magic";
    static final String FRAME_MAGIC = "frame.magic";
    static final String VERIFY_SCHEMA_COMPATIBILITY = "verify.schema.compatibility";
    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    private static final String AUTO_REGISTER_SCHEMAS = "auto.register.schemas";
    private static final String NORMALIZE_SCHEMAS = "normalize.schemas";
    private static final String MAX_SCHEMAS_PER_SUBJECT = "max.schemas.per.subject";
    private static final String FORCE_MIGRATION_NONCE = "force.migration.nonce";
    private static final String FLINK_CONFIG_PREFIX = "schema.resolving.avro.";
    private static final String SECRET_CONFIG_PREFIX = FLINK_CONFIG_PREFIX + "secret.";
    private static final int SCHEMA_REGISTRY_CACHE_CAPACITY = 1000;
    private static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;
    private static final int MAX_FRAME_MAGIC_BYTES = 16;
    private final String subject;
    private final AvroRecordKind recordKind;
    private final Class<T> specificClass;
    private final Schema readerSchema;
    private final Map<String, Object> serializerConfig;
    private final Schema legacyWriterSchema;
    private final byte[] frameMagic;
    private boolean legacyValueCanBeShorterThanMagic;
    private final boolean readOnly;
    private final Integer forceMigrationNonce;
    private Integer completedMigrationNonce;
    private boolean forceMigrationPending;

    private boolean schemaHistoryInitialized;
    private List<SchemaInfo> recordedSchemas = List.of();
    private transient SchemaRegistryClient schemaRegistryClient;
    private transient Map<Integer, DatumReader<T>> readersBySchemaId;
    private transient DatumReader<T> legacyReader;
    private transient Map<Schema, DatumWriter<T>> writersBySchema;
    private transient Map<Schema, Integer> schemaIdsBySchema;
    private transient DataInputDecoder legacyDecoder;
    private transient BinaryDecoder binaryDecoder;
    private transient BinaryEncoder binaryEncoder;

    private SchemaResolvingAvroSerializer(
            String subject,
            AvroRecordKind recordKind,
            Class<T> specificClass,
            Schema readerSchema,
            Map<String, ?> serializerConfig,
            Schema legacyWriterSchema,
            byte[] frameMagic,
            boolean readOnly) {
        this.subject = readOnly ? null : requireNonBlank(subject, "subject");
        this.recordKind = Objects.requireNonNull(recordKind, "recordKind");
        this.specificClass = specificClass;
        this.readerSchema = readerSchema;
        this.serializerConfig = normalizeConfig(serializerConfig);
        this.forceMigrationNonce = parseNonce(this.serializerConfig.get(FORCE_MIGRATION_NONCE));
        this.completedMigrationNonce = forceMigrationNonce;
        this.legacyWriterSchema = legacyWriterSchema;
        byte[] configuredMagic = configuredFrameMagic(this.serializerConfig);
        this.frameMagic = frameMagic != null ? frameMagic.clone()
                : configuredMagic != null ? configuredMagic : DEFAULT_FRAME_MAGIC;
        this.readOnly = readOnly;
    }

    static <T extends IndexedRecord> SchemaResolvingAvroSerializer<T> restoredReader(
            AvroRecordKind kind, Class<T> specificClass, Schema legacyWriterSchema, byte[] frameMagic, boolean canBeShorter) {
        Schema readerSchema = specificClass == null ? null : instantiate(specificClass).getSchema();
        SchemaResolvingAvroSerializer<T> reader = new SchemaResolvingAvroSerializer<>(null, kind, specificClass, readerSchema, Map.of(),
                legacyWriterSchema, frameMagic, true);
        reader.legacyValueCanBeShorterThanMagic = canBeShorter;
        return reader;
    }

    /** Creates a serializer for a generated Avro specific record using the literal Registry subject. */
    public static <T extends SpecificRecord> SchemaResolvingAvroSerializer<T> forSpecific(
            String subject, Class<T> recordClass, Map<String, ?> serializerConfig) {
        Objects.requireNonNull(recordClass, "recordClass");
        T instance = instantiate(recordClass);
        return new SchemaResolvingAvroSerializer<>(subject, AvroRecordKind.SPECIFIC, recordClass,
                instance.getSchema(), serializerConfig, null, null, false);
    }

    /** Creates a serializer for generic Avro records, projected to {@code readerSchema} on read. */
    public static SchemaResolvingAvroSerializer<GenericRecord> forGeneric(
            String subject, Schema readerSchema, Map<String, ?> serializerConfig) {
        Objects.requireNonNull(readerSchema, "readerSchema");
        return new SchemaResolvingAvroSerializer<>(subject, AvroRecordKind.GENERIC, null,
                readerSchema, serializerConfig, null, null, false);
    }

    /** A copy that reads raw values of {@code legacySchema} and frames new values with {@code magic}. */
    SchemaResolvingAvroSerializer<T> reconfigured(Schema legacySchema, byte[] magic) {
        return reconfigured(legacySchema, magic, legacySchema != null
                && LegacyFrameAnalysis.canBeShorterThanMagic(legacySchema, magic.length));
    }

    SchemaResolvingAvroSerializer<T> reconfigured(Schema legacySchema, byte[] magic, boolean canBeShorter) {
        SchemaResolvingAvroSerializer<T> copy = new SchemaResolvingAvroSerializer<>(
                subject, recordKind, specificClass, readerSchema, serializerConfig, legacySchema, magic, false);
        copy.legacyValueCanBeShorterThanMagic = canBeShorter;
        copy.schemaHistoryInitialized = schemaHistoryInitialized;
        copy.recordedSchemas = recordedSchemas;
        copy.completedMigrationNonce = completedMigrationNonce;
        copy.forceMigrationPending = forceMigrationPending;
        return copy;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        SchemaResolvingAvroSerializer<T> copy = new SchemaResolvingAvroSerializer<>(subject, recordKind,
                specificClass, readerSchema, serializerConfig, legacyWriterSchema, frameMagic, readOnly);
        copy.legacyValueCanBeShorterThanMagic = legacyValueCanBeShorterThanMagic;
        copy.schemaHistoryInitialized = schemaHistoryInitialized;
        copy.recordedSchemas = recordedSchemas;
        copy.completedMigrationNonce = completedMigrationNonce;
        copy.forceMigrationPending = forceMigrationPending;
        return copy;
    }

    @Override
    public T createInstance() {
        if (recordKind == AvroRecordKind.SPECIFIC) {
            return instantiate(specificClass);
        }
        if (readerSchema == null) {
            if (readOnly) return null;
            throw new IllegalStateException("A serializer has no generic reader schema");
        }
        @SuppressWarnings("unchecked") T record = (T) new GenericData.Record(readerSchema);
        return record;
    }

    @Override
    public T copy(T from) {
        if (from == null) {
            return null;
        }
        // Deep copies walk fields by position, so the copy must use the record's own schema, not the reader's.
        @SuppressWarnings("unchecked")
        T copy = (T) (recordKind == AvroRecordKind.SPECIFIC
                ? SpecificData.getForClass(specificClass).deepCopy(from.getSchema(), from)
                : GenericData.get().deepCopy(from.getSchema(), from));
        return copy;
    }

    @Override
    public T copy(T from, T reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        if (readOnly) throw new UnsupportedOperationException("Historical serializer is read-only");
        // Flink never hands a value serializer null: ValueState clears, ListState rejects, MapState flags it itself.
        Objects.requireNonNull(record, "record");
        T valueToWrite = record;
        Schema writerSchema = record.getSchema();
        // GenericRecord restore reads with each historical writer schema. During forced migration,
        // project those values here; SpecificRecord restore already reads into the current class/schema.
        if (forceMigrationPending && recordKind == AvroRecordKind.GENERIC && readerSchema != null
                && !writerSchema.equals(readerSchema)) {
            valueToWrite = projectToReaderSchema(record);
            writerSchema = readerSchema;
        }
        if (!writerSchema.equals(readerSchema)) {
            throw new IllegalArgumentException("Record schema must equal the configured runtime schema");
        }
        int schemaId = schemaIdForWrite(writerSchema);
        target.write(frameMagic);
        target.writeByte(WIRE_VERSION);
        writeVarInt(target, schemaId);
        // Framed datums use standard Avro binary encoding; only legacy values use Flink's DataOutputEncoder layout.
        binaryEncoder = EncoderFactory.get().directBinaryEncoder(new DataOutputViewStream(target), binaryEncoder);
        writerFor(writerSchema).write(valueToWrite, binaryEncoder);
        binaryEncoder.flush();
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        return deserialize(null, source);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        if (!legacyValueCanBeShorterThanMagic && frameMagic.length > 0) {
            byte[] probe = new byte[frameMagic.length];
            source.readFully(probe);
            if (!Arrays.equals(probe, frameMagic)) return deserializeLegacy(source, probe, reuse);
        } else {
            // Probe one byte at a time so a raw legacy datum loses at most the mismatching prefix, which is replayed.
            for (int index = 0; index < frameMagic.length; index++) {
                int next = source.readUnsignedByte();
                if ((byte) next != frameMagic[index]) {
                    byte[] consumed = Arrays.copyOf(frameMagic, index + 1);
                    consumed[index] = (byte) next;
                    return deserializeLegacy(source, consumed, reuse);
                }
            }
        }
        int version = source.readUnsignedByte();
        if (version != WIRE_VERSION) {
            throw new IOException("Unsupported schema-resolving Avro value format version: " + version);
        }
        int schemaId = readVarInt(source);
        if (schemaId <= 0) {
            throw new IOException("Invalid Schema Registry schema ID: " + schemaId);
        }
        // The direct decoder reads exactly the datum and never buffers ahead into the next value.
        binaryDecoder = DecoderFactory.get().directBinaryDecoder(new DataInputViewStream(source), binaryDecoder);
        return readerForSchemaId(schemaId).read(reuse, binaryDecoder);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        if (readOnly) throw new UnsupportedOperationException("Historical serializer is read-only");
        // The datum end is only known after decoding; re-encoding also frames legacy values with the current schema.
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        if (!readOnly) initializeSchemaHistory();
        return new SchemaResolvingAvroSerializerSnapshot<>(this);
    }

    boolean allowFullMigration() { return booleanConfig(ALLOW_FULL_MIGRATION, false); }
    boolean useUnsafeMagic() { return booleanConfig(USE_UNSAFE_MAGIC, false); }

    String subject() { return subject; }
    AvroRecordKind recordKind() { return recordKind; }
    Class<T> specificClass() { return specificClass; }
    Schema readerSchema() { return readerSchema; }
    Map<String, Object> serializerConfig() { return serializerConfig; }
    Schema legacyWriterSchema() { return legacyWriterSchema; }
    boolean legacyValueCanBeShorterThanMagic() { return legacyValueCanBeShorterThanMagic; }
    byte[] frameMagic() { return frameMagic.clone(); }
    byte[] configuredFrameMagic() { return configuredFrameMagic(serializerConfig); }
    Integer forceMigrationNonce() {
        return migrationNonceFromConfig(serializerConfig, GlobalConfiguration.loadConfiguration().toMap());
    }
    /** Falls back to the requested nonce so a fresh job records a Flink-config nonce instead of null. */
    Integer completedMigrationNonce() {
        return completedMigrationNonce != null ? completedMigrationNonce : forceMigrationNonce();
    }
    void recordMigrationNonce(Integer nonce) { completedMigrationNonce = nonce; }
    void markForceMigrationPending() { forceMigrationPending = true; }

    List<SchemaInfo> recordedSchemas() { return List.copyOf(recordedSchemas); }

    void recordedSchemas(List<SchemaInfo> schemas) { recordedSchemas = List.copyOf(schemas); schemaHistoryInitialized = false; }

    private void initializeSchemaHistory() {
        if (schemaHistoryInitialized) return;
        try {
            int id = schemaIdForWrite(readerSchema);
            int version = schemaRegistryClient().getVersion(subject, new AvroSchema(readerSchema),
                    booleanConfig(NORMALIZE_SCHEMAS, false));
            var entries = new java.util.TreeSet<SchemaInfo>(java.util.Comparator.comparing(SchemaInfo::subject)
                    .thenComparingInt(SchemaInfo::version).thenComparingInt(SchemaInfo::schemaId));
            entries.addAll(recordedSchemas);
            entries.add(new SchemaInfo(subject, version, id));
            recordedSchemas = List.copyOf(entries);
            schemaHistoryInitialized = true;
            LOG.debug("Schemas potentially present in state: {}", recordedSchemas);
            recordedSchemas.stream().map(SchemaInfo::subject).distinct().forEach(name ->
                    LOG.info("Subject '{}' has {} schema versions potentially present in state", name,
                            recordedSchemas.stream().filter(info -> info.subject().equals(name))
                                    .map(SchemaInfo::version).distinct().count()));
        } catch (IOException | RestClientException exception) {
            throw new IllegalStateException("Could not initialize writer schema history", exception);
        }
    }

    /** Returns null when the reader schema can read every known writer schema, else what it cannot read. */
    String verifyReaderCompatibility(Schema previousLegacySchema, String previousSubject) {
        if (!booleanConfig(VERIFY_SCHEMA_COMPATIBILITY, true)) {
            LOG.warn("Schema compatibility verification is disabled; values the reader schema cannot resolve fail when read");
            return null;
        }
        Map<String, Schema> writerSchemas = new LinkedHashMap<>();
        if (previousLegacySchema != null) {
            writerSchemas.put("legacy raw Avro writer schema '" + previousLegacySchema.getFullName() + "'", previousLegacySchema);
        }
        try {
            SchemaRegistryClient client = schemaRegistryClient();
            for (SchemaInfo info : recordedSchemas) {
                ParsedSchema parsed = client.getSchemaById(info.schemaId());
                if (!(parsed instanceof AvroSchema avro)) return "Recorded schema is not Avro: " + info;
                writerSchemas.put(info.toString(), avro.rawSchema());
            }

        } catch (IOException | RestClientException exception) {
            throw new IllegalStateException("Could not verify reader schema compatibility against Schema Registry", exception);
        }
        List<String> problems = new ArrayList<>();
        for (Map.Entry<String, Schema> writer : writerSchemas.entrySet()) {
            SchemaPairCompatibility result =
                    SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writer.getValue());
            if (result.getType() != SchemaCompatibilityType.COMPATIBLE) {
                problems.add(writer.getKey() + ": " + result.getDescription());
            }
        }
        if (!problems.isEmpty()) return String.join("; ", problems);
        try {
            schemaIdForWrite(readerSchema);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not resolve reader schema in Schema Registry", exception);
        }
        return null;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SchemaResolvingAvroSerializer<?> that)) {
            return false;
        }
        return Objects.equals(subject, that.subject)
                && readOnly == that.readOnly
                && recordKind == that.recordKind
                && Objects.equals(schemaString(readerSchema), schemaString(that.readerSchema))
                && serializerConfig.equals(that.serializerConfig)
                && Objects.equals(schemaString(legacyWriterSchema), schemaString(that.legacyWriterSchema))
                && Arrays.equals(frameMagic, that.frameMagic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, readOnly, recordKind, schemaString(readerSchema), serializerConfig,
                schemaString(legacyWriterSchema), Arrays.hashCode(frameMagic));
    }

    private static String schemaString(Schema schema) { return schema == null ? null : schema.toString(); }

    private T deserializeLegacy(DataInputView source, byte[] consumedPrefix, T reuse) throws IOException {
        if (legacyWriterSchema == null) {
            throw new IOException("Value does not start with frame magic " + LegacyFrameAnalysis.hex(frameMagic)
                    + " and no legacy Avro writer schema is known");
        }
        DataInput replay = new DataInputViewStreamWrapper(new PrefixInputStream(consumedPrefix, source));
        if (legacyDecoder == null) legacyDecoder = new DataInputDecoder();
        legacyDecoder.setIn(replay);
        return legacyReader().read(reuse, legacyDecoder);
    }

    private DatumReader<T> readerForSchemaId(int schemaId) throws IOException {
        if (readersBySchemaId == null) readersBySchemaId = new HashMap<>();
        DatumReader<T> reader = readersBySchemaId.get(schemaId);
        if (reader == null) {
            reader = newReader(writerSchemaForId(schemaId));
            readersBySchemaId.put(schemaId, reader);
        }
        return reader;
    }

    private DatumReader<T> legacyReader() {
        if (legacyReader == null) legacyReader = newReader(legacyWriterSchema);
        return legacyReader;
    }

    private DatumReader<T> newReader(Schema writerSchema) {
        if (recordKind == AvroRecordKind.SPECIFIC) {
            return new SpecificDatumReader<>(writerSchema, readerSchema, SpecificData.getForClass(specificClass));
        }
        // The historical generic reader keeps each value's own schema; the current serializer projects to its reader schema.
        return new GenericDatumReader<>(writerSchema, readOnly ? writerSchema : readerSchema);
    }

    private DatumWriter<T> writerFor(Schema writerSchema) {
        if (writersBySchema == null) writersBySchema = new HashMap<>();
        DatumWriter<T> writer = writersBySchema.get(writerSchema);
        if (writer == null) {
            if (recordKind == AvroRecordKind.SPECIFIC) {
                writer = new SpecificDatumWriter<>(writerSchema, SpecificData.getForClass(specificClass));
            } else {
                writer = new GenericDatumWriter<>(writerSchema);
            }
            writersBySchema.put(writerSchema, writer);
        }
        return writer;
    }

    /** Replays the probed bytes, then serves the rest of the value straight from the Flink input view. */
    private static final class PrefixInputStream extends InputStream {
        private final byte[] prefix;
        private final DataInputView source;
        private int index;

        private PrefixInputStream(byte[] prefix, DataInputView source) {
            this.prefix = prefix;
            this.source = source;
        }

        @Override
        public int read() throws IOException {
            if (index < prefix.length) return prefix[index++] & 0xff;
            try {
                return source.readUnsignedByte();
            } catch (EOFException end) {
                return -1;
            }
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (length == 0) return 0;
            if (index < prefix.length) {
                int count = Math.min(length, prefix.length - index);
                System.arraycopy(prefix, index, buffer, offset, count);
                index += count;
                return count;
            }
            return source.read(buffer, offset, length);
        }
    }

    private int schemaIdForWrite(Schema writerSchema) throws IOException {
        if (schemaIdsBySchema == null) schemaIdsBySchema = new HashMap<>();
        Integer cached = schemaIdsBySchema.get(writerSchema);
        if (cached != null) return cached;
        AvroSchema avroSchema = new AvroSchema(writerSchema);
        try {
            SchemaRegistryClient client = schemaRegistryClient();
            boolean normalize = booleanConfig(NORMALIZE_SCHEMAS, false);
            int schemaId = booleanConfig(AUTO_REGISTER_SCHEMAS, true)
                    ? client.register(subject, avroSchema, normalize)
                    : client.getId(subject, avroSchema, normalize);
            schemaIdsBySchema.put(writerSchema, schemaId);
            return schemaId;
        } catch (RestClientException exception) {
            throw new IOException("Could not resolve Avro schema ID for Schema Registry subject '" + subject + "'", exception);
        }
    }

    private Schema writerSchemaForId(int schemaId) throws IOException {
        try {
            ParsedSchema parsedSchema = schemaRegistryClient().getSchemaById(schemaId);
            if (!(parsedSchema instanceof AvroSchema avroSchema)) {
                throw new IOException("Schema Registry schema ID " + schemaId + " is not an Avro schema");
            }
            return avroSchema.rawSchema();
        } catch (RestClientException exception) {
            throw new IOException("Could not fetch Avro writer schema for Schema Registry ID " + schemaId, exception);
        }
    }

    private SchemaRegistryClient schemaRegistryClient() {
        if (schemaRegistryClient == null) {
            Map<String, Object> clientConfig = resolveClientConfig(
                    serializerConfig, GlobalConfiguration.loadConfiguration().toMap(), System.getenv());
            Object configuredUrl = clientConfig.get(SCHEMA_REGISTRY_URL);
            if (configuredUrl == null || configuredUrl.toString().isBlank()) {
                throw new IllegalStateException("Schema Registry config must include " + SCHEMA_REGISTRY_URL);
            }
            Object maxSchemas = clientConfig.remove(MAX_SCHEMAS_PER_SUBJECT);
            int cacheCapacity = maxSchemas == null ? SCHEMA_REGISTRY_CACHE_CAPACITY
                    : integerConfig(MAX_SCHEMAS_PER_SUBJECT, maxSchemas);
            LOG.debug("Initializing Schema Registry client for literal subject '{}' and record kind {}", subject, recordKind);
            schemaRegistryClient = SchemaRegistryClientFactory.newClient(configuredUrl.toString(), cacheCapacity,
                    List.of(new AvroSchemaProvider()), clientConfig, Map.of());
        }
        return schemaRegistryClient;
    }

    private T projectToReaderSchema(T record) throws IOException {
        Schema writerSchema = record.getSchema();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bytes, null);
        writerFor(writerSchema).write(record, encoder);
        encoder.flush();
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null);
        return newReader(writerSchema).read(null, decoder);
    }

    static void writeVarInt(DataOutputView out, int value) throws IOException {
        while ((value & ~0x7f) != 0) {
            out.writeByte((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value);
    }

    static int readVarInt(DataInputView in) throws IOException {
        int result = 0;
        for (int shift = 0; shift < 35; shift += 7) {
            int next = in.readUnsignedByte();
            result |= (next & 0x7f) << shift;
            if ((next & 0x80) == 0) return result;
        }
        throw new IOException("Malformed schema ID varint");
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static Map<String, Object> normalizeConfig(Map<String, ?> config) {
        Objects.requireNonNull(config, "serializerConfig");
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, ?> entry : config.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.matches("(?i).*password.*|.*secret.*|.*token.*|.*jaas.*|.*user.info.*")) {
                throw new IllegalArgumentException("Credentials must not be stored in a Flink serializer config: " + key);
            }
            if (!isSerializerConfigKey(key)) {
                LOG.warn("Unknown key in config present: {}", key);
                continue;
            }
            if (!(entry.getValue() instanceof Serializable)) {
                throw new IllegalArgumentException("Serializer config value must be Serializable: " + key);
            }
            copy.put(key, entry.getValue());
        }
        return Map.copyOf(copy);
    }

    private static boolean isSerializerConfigKey(String key) {
        return switch (key) {
            case SCHEMA_REGISTRY_URL, AUTO_REGISTER_SCHEMAS, NORMALIZE_SCHEMAS, MAX_SCHEMAS_PER_SUBJECT,
                    FORCE_MIGRATION_NONCE, FRAME_MAGIC, VERIFY_SCHEMA_COMPATIBILITY,
                    USE_UNSAFE_MAGIC, ALLOW_FULL_MIGRATION -> true;
            default -> false;
        };
    }

    static Map<String, Object> resolveClientConfig(
            Map<String, ?> serializerConfig,
            Map<String, String> flinkConfig,
            Map<String, String> environment) {
        Map<String, Object> resolved = new LinkedHashMap<>(serializerConfig);
        resolved.keySet().removeIf(SchemaResolvingAvroSerializer::isSerializerControlKey);

        Map<String, SecretReference> secretReferences = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : flinkConfig.entrySet()) {
            String flinkKey = entry.getKey();
            if (!flinkKey.startsWith(FLINK_CONFIG_PREFIX)) {
                continue;
            }
            if (flinkKey.startsWith(SECRET_CONFIG_PREFIX)) {
                addSecretReference(secretReferences, flinkKey.substring(SECRET_CONFIG_PREFIX.length()), entry.getValue());
                continue;
            }

            String clientKey = flinkKey.substring(FLINK_CONFIG_PREFIX.length());
            if (FORCE_MIGRATION_NONCE.equals(clientKey)) {
                continue;
            }
            if (isSerializerControlKey(clientKey)) {
                LOG.warn("Ignoring serializer-specific key in Flink Schema Registry client config: {}", flinkKey);
                continue;
            }
            resolved.putIfAbsent(clientKey, entry.getValue());
        }

        for (Map.Entry<String, SecretReference> entry : secretReferences.entrySet()) {
            SecretReference reference = entry.getValue();
            String secret;
            if (reference.environmentVariable() != null) {
                secret = environment.get(reference.environmentVariable());
                if (secret == null) {
                    throw new IllegalStateException("Schema Registry secret environment variable is not set: "
                            + reference.environmentVariable());
                }
            } else {
                try {
                    secret = Files.readString(Path.of(reference.filePath()), StandardCharsets.UTF_8).trim();
                } catch (IOException | RuntimeException exception) {
                    throw new IllegalStateException("Could not read Schema Registry secret file: "
                            + reference.filePath(), exception);
                }
            }
            if (secret.isBlank()) {
                throw new IllegalStateException("Schema Registry secret source resolved to an empty value for client property: "
                        + entry.getKey());
            }
            resolved.put(entry.getKey(), secret);
        }
        return resolved;
    }

    private static void addSecretReference(
            Map<String, SecretReference> secretReferences, String keyAndSource, String referenceValue) {
        if (keyAndSource.endsWith(".env")) {
            String clientKey = keyAndSource.substring(0, keyAndSource.length() - ".env".length());
            addSecretReference(secretReferences, clientKey, referenceValue, true);
        } else if (keyAndSource.endsWith(".file")) {
            String clientKey = keyAndSource.substring(0, keyAndSource.length() - ".file".length());
            addSecretReference(secretReferences, clientKey, referenceValue, false);
        } else {
            LOG.warn("Ignoring unrecognized Schema Registry secret reference key: {}{}",
                    SECRET_CONFIG_PREFIX, keyAndSource);
        }
    }

    private static void addSecretReference(
            Map<String, SecretReference> secretReferences, String clientKey, String referenceValue, boolean environment) {
        if (clientKey.isBlank() || referenceValue == null || referenceValue.isBlank()) {
            throw new IllegalArgumentException("Schema Registry secret references require a client property and source");
        }
        if (secretReferences.containsKey(clientKey)) {
            throw new IllegalArgumentException("Configure exactly one environment-variable or file secret source for client property: "
                    + clientKey);
        }
        secretReferences.put(clientKey, environment
                ? new SecretReference(referenceValue, null)
                : new SecretReference(null, referenceValue));
    }

    private record SecretReference(String environmentVariable, String filePath) { }

    private static boolean isSerializerControlKey(String key) {
        return switch (key) {
            case AUTO_REGISTER_SCHEMAS, NORMALIZE_SCHEMAS, FORCE_MIGRATION_NONCE, FRAME_MAGIC,
                    VERIFY_SCHEMA_COMPATIBILITY, USE_UNSAFE_MAGIC, ALLOW_FULL_MIGRATION -> true;
            default -> false;
        };
    }

    private boolean booleanConfig(String key, boolean defaultValue) {
        return booleanConfig(serializerConfig, key, defaultValue);
    }

    private static boolean booleanConfig(Map<String, ?> config, String key, boolean defaultValue) {
        Object value = config.get(key);
        if (value == null) return defaultValue;
        if (value instanceof Boolean booleanValue) return booleanValue;
        if (value instanceof String stringValue
                && (stringValue.equalsIgnoreCase("true") || stringValue.equalsIgnoreCase("false"))) {
            return Boolean.parseBoolean(stringValue);
        }
        throw new IllegalArgumentException("Config '" + key + "' must be a boolean");
    }

    static byte[] configuredFrameMagic(Map<String, ?> config) {
        Object value = config.get(FRAME_MAGIC);
        if (value == null) return null;
        String hex = value.toString().trim();
        if (!hex.matches("([0-9A-Fa-f]{2}){1," + MAX_FRAME_MAGIC_BYTES + "}")) {
            throw new IllegalArgumentException("Config '" + FRAME_MAGIC + "' must be 1 to " + MAX_FRAME_MAGIC_BYTES
                    + " bytes as hex, for example FF or 53524156524F");
        }
        byte[] magic = new byte[hex.length() / 2];
        for (int index = 0; index < magic.length; index++) {
            magic[index] = (byte) Integer.parseInt(hex.substring(2 * index, 2 * index + 2), 16);
        }
        return magic;
    }

    static Integer migrationNonceFromConfig(Map<String, ?> serializerConfig, Map<String, ?> flinkConfig) {
        Object mapValue = serializerConfig.get(FORCE_MIGRATION_NONCE);
        return parseNonce(mapValue != null ? mapValue : flinkConfig.get(FLINK_CONFIG_PREFIX + FORCE_MIGRATION_NONCE));
    }

    private static Integer parseNonce(Object value) {
        if (value == null) return null;
        try {
            return Integer.parseInt(value.toString().trim());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Config '" + FORCE_MIGRATION_NONCE + "' must be a 32-bit integer", exception);
        }
    }

    private static int integerConfig(String key, Object value) {
        try {
            int result = Integer.parseInt(value.toString());
            if (result <= 0) throw new NumberFormatException("must be positive");
            return result;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Config '" + key + "' must be a positive integer", exception);
        }
    }

    private static <T> T instantiate(Class<T> type) {
        try {
            return type.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException exception) {
            throw new IllegalArgumentException("Specific record needs an accessible no-argument constructor: " + type.getName(), exception);
        }
    }
}
