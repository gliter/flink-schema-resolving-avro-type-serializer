package io.github.gliter.flink.avro;

import java.io.Serializable;

/** A configured writer schema potentially present in state. */
record SchemaInfo(String subject, int version, int schemaId) implements Serializable { }
