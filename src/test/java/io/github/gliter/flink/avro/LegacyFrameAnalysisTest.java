package io.github.gliter.flink.avro;

import io.github.gliter.flink.avro.LegacyFrameAnalysis.FrameMagicChoice;
import io.github.gliter.flink.avro.LegacyFrameAnalysis.MagicSafety;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HexFormat;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacyFrameAnalysisTest {
    private static final Schema STRING_FIRST = record("{\"name\":\"id\",\"type\":\"string\"}");
    private static final Schema UNION_FIRST = record("{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null}");
    private static final Schema BOOLEAN_ONLY = record("{\"name\":\"flag\",\"type\":\"boolean\"}");
    private static final Schema INT_ONLY = record("{\"name\":\"count\",\"type\":\"int\"}");
    private static final Schema LONG_AND_DOUBLE = record(
            "{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"value\",\"type\":\"double\"}");
    private static final Schema ARRAY_FIRST = record("{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}");
    private static final Schema NULL_ONLY = record("{\"name\":\"nothing\",\"type\":\"null\"}");
    private static final Schema RECURSIVE = record(
            "{\"name\":\"value\",\"type\":\"int\"},{\"name\":\"next\",\"type\":[\"null\",\"Outer\"]}");

    @org.junit.jupiter.api.Test
    void bulkProbeRequiresEveryLegacyValueToFitTheMagic() {
        assertFalse(LegacyFrameAnalysis.canBeShorterThanMagic(STRING_FIRST, 1));
        assertTrue(LegacyFrameAnalysis.canBeShorterThanMagic(ARRAY_FIRST, 5));
        assertFalse(LegacyFrameAnalysis.canBeShorterThanMagic(LONG_AND_DOUBLE, 6));
        assertTrue(LegacyFrameAnalysis.canBeShorterThanMagic(NULL_ONLY, 1));
        assertFalse(LegacyFrameAnalysis.canBeShorterThanMagic(NULL_ONLY, 0));
    }

    static Stream<Arguments> safetyChecks() {
        return Stream.of(
                Arguments.of("string first never starts above 0x7F", STRING_FIRST, "FF", false, false),
                Arguments.of("string first can start with a zero length byte", STRING_FIRST, "00", true, false),
                Arguments.of("union first never starts above 0x7F", UNION_FIRST, "FF", false, false),
                Arguments.of("union index out of range is impossible", UNION_FIRST, "00000002", false, false),
                Arguments.of("union index in range collides", UNION_FIRST, "00000001", true, false),
                Arguments.of("boolean value is a proper prefix of a longer magic", BOOLEAN_ONLY, "0000", false, true),
                Arguments.of("boolean never starts with 0xFF", BOOLEAN_ONLY, "FF", false, false),
                Arguments.of("int value collides with any four bytes", INT_ONLY, "FF", true, false),
                Arguments.of("int value is a proper prefix of six bytes", INT_ONLY, "53524156524F", false, true),
                Arguments.of("array count can continue through four 0xFF bytes", ARRAY_FIRST, "FFFFFFFF", true, false),
                Arguments.of("array count cannot take five continuation bytes", ARRAY_FIRST, "FFFFFFFFFF", false, false),
                Arguments.of("empty value ends before any magic", NULL_ONLY, "FF", false, true),
                Arguments.of("recursive record continues into the nested record", RECURSIVE, "FFFFFFFF00000001FF", true, false),
                Arguments.of("recursive record stops at an out-of-range union index", RECURSIVE, "FFFFFFFF00000002", false, false));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("safetyChecks")
    void checksCollisionAndOverRead(String name, Schema schema, String magicHex, boolean collision, boolean overRead) {
        MagicSafety safety = LegacyFrameAnalysis.check(schema, HexFormat.of().parseHex(magicHex));
        assertEquals(collision, safety.collisionPossible(), "collision");
        assertEquals(overRead, safety.overReadPossible(), "over-read");
    }

    static Stream<Arguments> frameMagicChoices() {
        return Stream.of(
                Arguments.of("string first gets one byte", STRING_FIRST, null, "FF"),
                Arguments.of("union first gets one byte", UNION_FIRST, null, "FF"),
                Arguments.of("array first needs five bytes", ARRAY_FIRST, null, "FFFFFFFFFF"),
                Arguments.of("numeric values require an explicit policy",
                        LONG_AND_DOUBLE, null, null),
                Arguments.of("four-byte values have no safe magic", INT_ONLY, null, null),
                Arguments.of("possibly empty values have no safe magic", NULL_ONLY, null, null),
                Arguments.of("configured collision-free magic is kept", STRING_FIRST, "AB", "AB"),
                Arguments.of("configured colliding magic requires unsafe opt-in", STRING_FIRST, "00", null),
                Arguments.of("configured magic that can over-read migrates", BOOLEAN_ONLY, "0000", null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("frameMagicChoices")
    void choosesFrameMagicOrMigration(String name, Schema legacySchema, String configuredHex, String expectedHex) {
        byte[] configured = configuredHex == null ? null : HexFormat.of().parseHex(configuredHex);
        FrameMagicChoice choice = LegacyFrameAnalysis.chooseFrameMagic(legacySchema, configured);
        assertFalse(choice.explanation().isBlank());
        if (expectedHex == null) {
            assertTrue(choice.requiresMigration());
            assertNull(choice.magic());
        } else {
            assertFalse(choice.requiresMigration());
            assertArrayEquals(HexFormat.of().parseHex(expectedHex), choice.magic());
        }
    }

    static Stream<Arguments> flinkEncodings() {
        GenericRecord text = new GenericData.Record(STRING_FIRST);
        text.put("id", "ab");
        GenericRecord union = new GenericData.Record(UNION_FIRST);
        union.put("id", "x");
        GenericRecord count = new GenericData.Record(INT_ONLY);
        count.put("count", -1);
        GenericRecord tags = new GenericData.Record(ARRAY_FIRST);
        tags.put("tags", java.util.List.of("a"));
        return Stream.of(
                Arguments.of("string is a four-byte length then bytes", text, "000000026162"),
                Arguments.of("union index is four bytes", union, "0000000100000001" + "78"),
                Arguments.of("int is four big-endian bytes", count, "FFFFFFFF"),
                Arguments.of("array is a varint count, items and a zero", tags, "01" + "000000016100"));
    }

    /** Pins the Flink encoder layout the analysis depends on; a change here must be mirrored in the walker. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("flinkEncodings")
    void flinkAvroSerializerLayoutMatchesTheWalkerModel(String name, GenericRecord record, String expectedHex) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(32);
        new AvroSerializer<>(GenericRecord.class, record.getSchema()).serialize(record, output);
        assertEquals(expectedHex.toUpperCase(), HexFormat.of().withUpperCase().formatHex(output.getCopyOfBuffer()));
    }

    private static Schema record(String fields) {
        return new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Outer\",\"fields\":[" + fields + "]}");
    }
}
