package io.github.gliter.flink.avro.model;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

/** Minimal generated-record equivalent used to verify specific-record serialization and reuse. */
public final class Greeting extends SpecificRecordBase {
    public static final Schema SCHEMA$ = new Schema.Parser().parse("""
            {"type":"record","name":"Greeting","namespace":"io.github.gliter.flink.avro.model",
             "fields":[{"name":"message","type":"string"}]}
            """);
    private CharSequence message;

    public Greeting() { }

    public Greeting(String message) {
        this.message = message;
    }

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    public Object get(int field) {
        if (field == 0) {
            return message;
        }
        throw new IndexOutOfBoundsException("field " + field);
    }

    @Override
    public void put(int field, Object value) {
        if (field == 0) {
            message = (CharSequence) value;
            return;
        }
        throw new IndexOutOfBoundsException("field " + field);
    }

    public String message() {
        return message.toString();
    }
}
