package io.github.gliter.flink.avro;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.formats.avro.utils.DataInputDecoder;
import org.apache.flink.formats.avro.utils.DataOutputEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A Flink state serializer that stores Confluent-framed Avro bytes.
 *
 * <p>Each non-null value retains its Confluent schema id. This allows a current reader schema to
 * resolve historical bytes without rewriting otherwise stable state bytes.</p>
 *
 * @param <T> the Avro record representation
 */
public final class SchemaResolvingAvroSerializer<T extends IndexedRecord> extends TypeSerializer<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaResolvingAvroSerializer.class);
    static final byte[] WIRE_MAGIC = new byte[] { 'S', 'R', 'A', 'V', 'R', 'O' };
    static final byte WIRE_VERSION = 1;
    private static final int WIRE_MAGIC_WORD = ((WIRE_MAGIC[0] & 0xff) << 24)
            | ((WIRE_MAGIC[1] & 0xff) << 16) | ((WIRE_MAGIC[2] & 0xff) << 8) | (WIRE_MAGIC[3] & 0xff);
    private static final int WIRE_MAGIC_SUFFIX_WORD = ((WIRE_MAGIC[4] & 0xff) << 8) | (WIRE_MAGIC[5] & 0xff);
    private static final String AUTO_REGISTER_SCHEMAS = "auto.register.schemas";
    private static final String NORMALIZE_SCHEMAS = "normalize.schemas";
    private static final String FORCE_MIGRATION_NONCE = "force.migration.nonce";
    private static final String FLINK_CONFIG_PREFIX = "schema.resolving.avro.";
    private static final String SECRET_CONFIG_PREFIX = FLINK_CONFIG_PREFIX + "secret.";
    private static final int SCHEMA_REGISTRY_CACHE_CAPACITY = 1000;
    private final String subject;
    private final AvroRecordKind recordKind;
    private final Class<T> specificClass;
    private final Schema readerSchema;
    private final Map<String, Object> serializerConfig;
    private final Schema legacyWriterSchema;
    private final boolean readOnly;
    private final Integer forceMigrationNonce;
    private Integer completedMigrationNonce;
    private boolean forceMigrationPending;

    private transient SchemaRegistryClient schemaRegistryClient;

    private SchemaResolvingAvroSerializer(
            String subject,
            AvroRecordKind recordKind,
            Class<T> specificClass,
            Schema readerSchema,
            Map<String, ?> serializerConfig,
            Schema legacyWriterSchema) {
        this(subject, recordKind, specificClass, readerSchema, serializerConfig, legacyWriterSchema, false);
    }

    private SchemaResolvingAvroSerializer(
            String subject,
            AvroRecordKind recordKind,
            Class<T> specificClass,
            Schema readerSchema,
            Map<String, ?> serializerConfig,
            Schema legacyWriterSchema,
            boolean readOnly) {
        this.subject = readOnly ? null : requireNonBlank(subject, "subject");
        this.recordKind = Objects.requireNonNull(recordKind, "recordKind");
        this.specificClass = specificClass;
        this.readerSchema = readerSchema;
        this.serializerConfig = normalizeConfig(serializerConfig);
        this.forceMigrationNonce = integerNonceConfig(this.serializerConfig, FORCE_MIGRATION_NONCE);
        this.completedMigrationNonce = forceMigrationNonce;
        this.legacyWriterSchema = legacyWriterSchema;
        this.readOnly = readOnly;
    }

    static <T extends IndexedRecord> SchemaResolvingAvroSerializer<T> restoredReader(
            AvroRecordKind kind, Class<T> specificClass, Schema legacyWriterSchema) {
        Schema readerSchema = specificClass == null ? null : instantiate(specificClass).getSchema();
        return new SchemaResolvingAvroSerializer<>(null, kind, specificClass, readerSchema, Map.of(),
                legacyWriterSchema, true);
    }

    /** Creates a serializer for a generated Avro specific record using the literal Registry subject. */
    public static <T extends SpecificRecord> SchemaResolvingAvroSerializer<T> forSpecific(
            String subject, Class<T> recordClass, Map<String, ?> serializerConfig) {
        Objects.requireNonNull(recordClass, "recordClass");
        T instance = instantiate(recordClass);
        return new SchemaResolvingAvroSerializer<>(subject, AvroRecordKind.SPECIFIC, recordClass,
                instance.getSchema(), serializerConfig, null);
    }

    /** Creates a serializer for generic Avro records, projected to {@code readerSchema} on read. */
    public static SchemaResolvingAvroSerializer<GenericRecord> forGeneric(
            String subject, Schema readerSchema, Map<String, ?> serializerConfig) {
        Objects.requireNonNull(readerSchema, "readerSchema");
        return new SchemaResolvingAvroSerializer<>(subject, AvroRecordKind.GENERIC, null,
                readerSchema, serializerConfig, null);
    }

    SchemaResolvingAvroSerializer<T> withLegacyWriterSchema(Schema schema) {
        SchemaResolvingAvroSerializer<T> copy = new SchemaResolvingAvroSerializer<>(
                subject, recordKind, specificClass, readerSchema, serializerConfig, schema);
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
        SchemaResolvingAvroSerializer<T> copy = new SchemaResolvingAvroSerializer<>(
                subject, recordKind, specificClass, readerSchema, serializerConfig, legacyWriterSchema, readOnly);
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
        @SuppressWarnings("unchecked")
        T copy = (T) (recordKind == AvroRecordKind.SPECIFIC
                ? SpecificData.getForClass(specificClass).deepCopy(readerSchema, from)
                : GenericData.get().deepCopy(readerSchema == null ? from.getSchema() : readerSchema, from));
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
        target.write(WIRE_MAGIC);
        target.writeByte(WIRE_VERSION);
        if (record == null) {
            target.writeBoolean(false);
            return;
        }
        T valueToWrite = record;
        Schema writerSchema = record.getSchema();
        // GenericRecord restore reads with each historical writer schema. During forced migration,
        // project those values here; SpecificRecord restore already reads into the current class/schema.
        if (forceMigrationPending && recordKind == AvroRecordKind.GENERIC && readerSchema != null
                && !writerSchema.equals(readerSchema)) {
            valueToWrite = projectToReaderSchema(record);
            writerSchema = readerSchema;
        }
        AvroSchema avroWriterSchema = new AvroSchema(writerSchema);
        int schemaId = schemaIdForWrite(avroWriterSchema);
        byte[] datum = encodeDatum(valueToWrite, writerSchema);
        target.writeBoolean(true);
        target.writeInt(datum.length + 1 + Integer.BYTES);
        target.writeByte(0);
        target.writeInt(schemaId);
        target.write(datum);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        return deserialize(null, source);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        // Probe four bytes first so legacy raw-Avro values can be replayed without consuming them.
        // A null framed value's full header is eight bytes, but an old Avro datum has no such header.
        int firstWord = source.readInt();
        if (firstWord != WIRE_MAGIC_WORD) return deserializeLegacy(source, firstWord, reuse);
        int magicSuffix = source.readUnsignedShort();
        if (magicSuffix != WIRE_MAGIC_SUFFIX_WORD) {
            return deserializeLegacy(source, firstWord, magicSuffix, reuse);
        }
        int version = source.readUnsignedByte();
        if (version != WIRE_VERSION) throw new IOException("Unsupported Confluent Avro value format version: " + version);
        if (!source.readBoolean()) return null;
        int length = source.readInt();
        if (length < 1 + Integer.BYTES) {
            throw new IOException("Invalid Confluent payload length: " + length);
        }
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        if (bytes[0] != 0) {
            throw new IOException("Unsupported Confluent Avro schema-ID wire-format version: " + (bytes[0] & 0xff));
        }
        int schemaId = ((bytes[1] & 0xff) << 24) | ((bytes[2] & 0xff) << 16)
                | ((bytes[3] & 0xff) << 8) | (bytes[4] & 0xff);
        if (schemaId <= 0) {
            throw new IOException("Invalid Confluent Avro schema ID: " + schemaId);
        }
        Schema writerSchema = writerSchemaForId(schemaId);
        ByteArrayInputStream datumBytes = new ByteArrayInputStream(bytes, 1 + Integer.BYTES,
                bytes.length - 1 - Integer.BYTES);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(datumBytes, null);
        return readDatum(writerSchema, reuse, decoder);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        if (readOnly) throw new UnsupportedOperationException("Historical serializer is read-only");
        // Keep this four-byte probe aligned with deserialize(): a null value has no eight-byte header.
        int firstWord = source.readInt();
        if (firstWord != WIRE_MAGIC_WORD) {
            T record = deserializeLegacy(source, firstWord, null);
            writeLegacy(record, target);
            return;
        }
        int magicSuffix = source.readUnsignedShort();
        if (magicSuffix != WIRE_MAGIC_SUFFIX_WORD) {
            T record = deserializeLegacy(source, firstWord, magicSuffix, null);
            writeLegacy(record, target);
            return;
        }
        int version = source.readUnsignedByte();
        if (version != WIRE_VERSION) throw new IOException("Unsupported Confluent Avro value format version: " + version);
        target.write(WIRE_MAGIC);
        target.writeByte(WIRE_VERSION);
        boolean present = source.readBoolean();
        target.writeBoolean(present);
        if (!present) {
            return;
        }
        int length = source.readInt();
        if (length < 1 + Integer.BYTES) {
            throw new IOException("Invalid Confluent payload length: " + length);
        }
        target.writeInt(length);
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        target.write(bytes);
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new SchemaResolvingAvroSerializerSnapshot<>(this);
    }

    String subject() { return subject; }
    AvroRecordKind recordKind() { return recordKind; }
    Class<T> specificClass() { return specificClass; }
    Schema readerSchema() { return readerSchema; }
    Map<String, Object> serializerConfig() { return serializerConfig; }
    Schema legacyWriterSchema() { return legacyWriterSchema; }
    Integer forceMigrationNonce() {
        return migrationNonceFromConfig(serializerConfig, GlobalConfiguration.loadConfiguration().toMap());
    }
    Integer completedMigrationNonce() { return completedMigrationNonce; }
    void recordMigrationNonce(Integer nonce) { completedMigrationNonce = nonce; }
    void markForceMigrationPending() { forceMigrationPending = true; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SchemaResolvingAvroSerializer<?> that)) {
            return false;
        }
        return Objects.equals(subject, that.subject)
                && readOnly == that.readOnly
                && recordKind == that.recordKind
                && Objects.equals(readerSchema == null ? null : readerSchema.toString(),
                        that.readerSchema == null ? null : that.readerSchema.toString())
                && serializerConfig.equals(that.serializerConfig)
                && Objects.equals(schemaString(legacyWriterSchema), schemaString(that.legacyWriterSchema));
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, readOnly, recordKind, schemaString(readerSchema), serializerConfig,
                schemaString(legacyWriterSchema));
    }

    private static String schemaString(Schema schema) { return schema == null ? null : schema.toString(); }

    private T deserializeLegacy(DataInputView source, int firstWord, T reuse) throws IOException {
        return deserializeLegacy(source, firstWordPrefix(firstWord), reuse);
    }

    private T deserializeLegacy(DataInputView source, int firstWord, int secondWord, T reuse) throws IOException {
        byte[] prefix = new byte[] {
                (byte) (firstWord >>> 24), (byte) (firstWord >>> 16), (byte) (firstWord >>> 8), (byte) firstWord,
                (byte) (secondWord >>> 8), (byte) secondWord
        };
        return deserializeLegacy(source, prefix, reuse);
    }

    private T deserializeLegacy(DataInputView source, byte[] prefix, T reuse) throws IOException {
        if (legacyWriterSchema == null) {
            throw new IOException("Unframed legacy Avro state requires a writer schema from AvroSerializerSnapshot");
        }
        DataInputView replay = new DataInputViewStreamWrapper(new PrefixInputStream(prefix, source));
        DataInputDecoder decoder = new DataInputDecoder();
        decoder.setIn(replay);
        return readDatum(legacyWriterSchema, reuse, decoder);
    }

    private static byte[] firstWordPrefix(int firstWord) {
        return new byte[] {
                (byte) (firstWord >>> 24), (byte) (firstWord >>> 16),
                (byte) (firstWord >>> 8), (byte) firstWord
        };
    }

    private void writeLegacy(T record, DataOutputView target) throws IOException {
        DataOutputEncoder encoder = new DataOutputEncoder();
        encoder.setOut(target);
        if (recordKind == AvroRecordKind.SPECIFIC) {
            new org.apache.avro.specific.SpecificDatumWriter<T>(legacyWriterSchema,
                    SpecificData.getForClass(specificClass)).write(record, encoder);
        } else {
            new GenericDatumWriter<T>(legacyWriterSchema).write(record, encoder);
        }
        encoder.flush();
    }

    private static final class PrefixInputStream extends InputStream {
        private final byte[] prefix;
        private final DataInputView source;
        private int index;
        private PrefixInputStream(byte[] prefix, DataInputView source) { this.prefix = prefix; this.source = source; }
        @Override public int read() throws IOException {
            if (index < prefix.length) return prefix[index++] & 0xff;
            try { return source.readUnsignedByte(); } catch (java.io.EOFException end) { return -1; }
        }
    }

    private int schemaIdForWrite(AvroSchema schema) throws IOException {
        try {
            SchemaRegistryClient client = schemaRegistryClient();
            if (booleanConfig(AUTO_REGISTER_SCHEMAS, true)) {
                return client.register(subject, schema, booleanConfig(NORMALIZE_SCHEMAS, false));
            }
            return client.getId(subject, schema, booleanConfig(NORMALIZE_SCHEMAS, false));
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
            Object configuredUrl = clientConfig.get("schema.registry.url");
            if (configuredUrl == null || configuredUrl.toString().isBlank()) {
                throw new IllegalStateException("Schema Registry config must include schema.registry.url");
            }
            Object maxSchemas = clientConfig.remove("max.schemas.per.subject");
            int cacheCapacity = maxSchemas == null ? SCHEMA_REGISTRY_CACHE_CAPACITY
                    : integerConfig("max.schemas.per.subject", maxSchemas);
            LOG.debug("Initializing Schema Registry client for literal subject '{}' and record kind {}", subject, recordKind);
            schemaRegistryClient = SchemaRegistryClientFactory.newClient(configuredUrl.toString(), cacheCapacity,
                    List.of(new AvroSchemaProvider()), clientConfig, Map.of());
        }
        return schemaRegistryClient;
    }

    private byte[] encodeDatum(T record, Schema writerSchema) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bytes, null);
        if (recordKind == AvroRecordKind.SPECIFIC) {
            @SuppressWarnings("unchecked")
            SpecificRecord specificRecord = (SpecificRecord) record;
            new SpecificDatumWriter<SpecificRecord>(writerSchema, SpecificData.getForClass(specificClass))
                    .write(specificRecord, encoder);
        } else {
            new GenericDatumWriter<T>(writerSchema).write(record, encoder);
        }
        encoder.flush();
        return bytes.toByteArray();
    }

    private T projectToReaderSchema(T record) throws IOException {
        Schema writerSchema = record.getSchema();
        if (writerSchema.equals(readerSchema)) {
            return record;
        }
        byte[] encoded = encodeDatum(record, writerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(encoded), null);
        return readDatum(writerSchema, null, decoder);
    }

    @SuppressWarnings("unchecked")
    private T readDatum(Schema writerSchema, T reuse, org.apache.avro.io.Decoder decoder) throws IOException {
        if (recordKind == AvroRecordKind.SPECIFIC) {
            return new SpecificDatumReader<T>(writerSchema, readerSchema,
                    SpecificData.getForClass(specificClass)).read(reuse, decoder);
        }
        GenericRecord genericReuse = (GenericRecord) reuse;
        Schema genericReaderSchema = readOnly ? writerSchema : readerSchema;
        return (T) new GenericDatumReader<GenericRecord>(writerSchema, genericReaderSchema).read(genericReuse, decoder);
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
            case "schema.registry.url", AUTO_REGISTER_SCHEMAS, NORMALIZE_SCHEMAS,
                    FORCE_MIGRATION_NONCE, "max.schemas.per.subject" -> true;
            default -> false;
        };
    }

    static Map<String, Object> resolveClientConfig(
            Map<String, ?> serializerConfig,
            Map<String, String> flinkConfig,
            Map<String, String> environment) {
        Map<String, Object> resolved = new LinkedHashMap<>(serializerConfig);
        resolved.remove(AUTO_REGISTER_SCHEMAS);
        resolved.remove(NORMALIZE_SCHEMAS);
        resolved.remove(FORCE_MIGRATION_NONCE);

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
        return AUTO_REGISTER_SCHEMAS.equals(key)
                || NORMALIZE_SCHEMAS.equals(key)
                || FORCE_MIGRATION_NONCE.equals(key);
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

    private static Integer integerNonceConfig(Map<String, ?> config, String key) {
        Object value = config.get(key);
        if (value == null) return null;
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Config '" + key + "' must be a number");
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Config '" + key + "' must be a 32-bit integer", exception);
        }
    }

    static Integer migrationNonceFromConfig(
            Map<String, ?> serializerConfig, Map<String, ?> flinkConfig) {
        Object mapValue = serializerConfig.get(FORCE_MIGRATION_NONCE);
        Object configuredValue = mapValue != null
                ? mapValue
                : flinkConfig.get(FLINK_CONFIG_PREFIX + FORCE_MIGRATION_NONCE);
        if (configuredValue == null) return null;
        try {
            if (configuredValue instanceof Number number) {
                return Integer.parseInt(number.toString());
            }
            return Integer.parseInt(configuredValue.toString());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Config '" + FORCE_MIGRATION_NONCE
                    + "' must be a 32-bit integer", exception);
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
