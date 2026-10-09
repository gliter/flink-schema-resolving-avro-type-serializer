package io.github.gliter.flink.avro;

import java.util.Map;

/** Avro representation returned from the Confluent deserializer. */
enum AvroRecordKind {
    /** Generated Avro {@code SpecificRecord} classes. */
    SPECIFIC(1),
    /** {@code GenericRecord} values projected onto a caller-supplied reader schema. */
    GENERIC(2);

    private static final Map<Integer, AvroRecordKind> BY_SNAPSHOT_ID = Map.of(
            1, SPECIFIC,
            2, GENERIC);

    private final int snapshotId;

    AvroRecordKind(int snapshotId) {
        this.snapshotId = snapshotId;
    }

    int snapshotId() {
        return snapshotId;
    }

    static AvroRecordKind fromSnapshotId(int snapshotId) {
        AvroRecordKind kind = BY_SNAPSHOT_ID.get(snapshotId);
        if (kind == null) {
            throw new IllegalArgumentException("Unknown Avro record kind snapshot ID: " + snapshotId);
        }
        return kind;
    }
}
