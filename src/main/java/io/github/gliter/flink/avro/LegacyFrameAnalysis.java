package io.github.gliter.flink.avro;

import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

/**
 * Decides whether a frame magic can be told apart from raw values written by Flink's {@code AvroSerializer}.
 *
 * <p>Flink's {@code DataOutputEncoder} is not Avro binary encoding: int, float, enum and union indexes are
 * 4 big-endian bytes, long and double are 8, strings and bytes carry a 4-byte big-endian length, and only
 * array and map item counts are unsigned varints. The walker below replays every byte sequence that encoder
 * can emit for a schema, as far as a candidate magic reaches, to prove two properties: no legacy value starts
 * with the whole magic (no collision), and no legacy value is a proper prefix of it (no over-read).</p>
 */
final class LegacyFrameAnalysis {
    private static final int MAX_AUTOMATIC_MAGIC_BYTES = 8;
    private static final int STEP_BUDGET = 100_000;
    private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);

    private LegacyFrameAnalysis() { }

    /** Whether a legacy value can start with the whole magic, or end before the magic does. */
    record MagicSafety(boolean collisionPossible, boolean overReadPossible) {
        boolean provablySafe() {
            return !collisionPossible && !overReadPossible;
        }
    }

    /** The frame magic to use for state that still holds raw legacy values, or why a rewrite is unavoidable. */
    record FrameMagicChoice(byte[] magic, String explanation, boolean requiresMigration) {
        static FrameMagicChoice of(byte[] magic, String explanation) {
            return new FrameMagicChoice(magic, explanation, false);
        }

        static FrameMagicChoice migration(String explanation) {
            return new FrameMagicChoice(null, explanation, true);
        }
    }

    static MagicSafety check(Schema legacyWriterSchema, byte[] magic) {
        return new Walker(magic).run(legacyWriterSchema);
    }

    static FrameMagicChoice chooseFrameMagic(Schema legacyWriterSchema, byte[] configuredMagic) {
        return chooseFrameMagic(legacyWriterSchema, configuredMagic, false);
    }

    static FrameMagicChoice chooseFrameMagic(Schema legacyWriterSchema, byte[] configuredMagic, boolean useUnsafeMagic) {
        String schemaName = legacyWriterSchema.getFullName();
        if (configuredMagic != null) {
            MagicSafety safety = check(legacyWriterSchema, configuredMagic);
            String hex = hex(configuredMagic);
            if (safety.overReadPossible()) {
                return FrameMagicChoice.migration("a legacy value of schema '" + schemaName
                        + "' can be a proper prefix of configured frame magic " + hex
                        + ", so reading it would consume bytes past its end");
            }
            if (safety.collisionPossible() && !useUnsafeMagic) {
                return FrameMagicChoice.migration("configured magic " + hex + " may collide with legacy values");
            }
            if (safety.collisionPossible()) {
                return FrameMagicChoice.of(configuredMagic, "configured frame magic " + hex
                        + " may also begin a legacy value of schema '" + schemaName
                        + "'; detection relies on that not happening by coincidence");
            }
            return FrameMagicChoice.of(configuredMagic, "configured frame magic " + hex
                    + " can never begin a legacy value of schema '" + schemaName + "'");
        }
        Prefix prefix = automaticPrefix(legacyWriterSchema);
        if (prefix.isSafeMagic() && prefix.bytes() <= MAX_AUTOMATIC_MAGIC_BYTES) {
            byte[] magic = new byte[prefix.bytes()];
            Arrays.fill(magic, (byte) 0xff);
            return FrameMagicChoice.of(magic, "legacy schema '" + schemaName
                    + "' excludes an all-FF prefix of " + magic.length + " bytes");
        }
        if (!useUnsafeMagic) {
            return FrameMagicChoice.migration("no proven safe automatic magic for legacy schema '" + schemaName + "'");
        }
        byte[] heuristicMagic = SchemaResolvingAvroSerializer.HEURISTIC_FRAME_MAGIC;
        if (!check(legacyWriterSchema, heuristicMagic).overReadPossible()) {
            return FrameMagicChoice.of(heuristicMagic, "legacy values of schema '" + schemaName
                    + "' can begin with any short byte sequence; using the " + heuristicMagic.length
                    + "-byte frame magic " + hex(heuristicMagic) + ", which a legacy value can only match by coincidence");
        }
        return FrameMagicChoice.migration("legacy values of schema '" + schemaName
                + "' can be a proper prefix of every candidate frame magic, so none is safe");
    }

    /**
     * Derives the first impossible all-FF prefix directly from the encoding layout.
     * Unconstrained fixed-width fields advance the offset; a constrained field ends the search.
     * Lengths above our eight-byte limit are capped to avoid traversing irrelevant schema tails.
     */
    private record Prefix(int bytes, boolean isSafeMagic) { }

    private static Prefix automaticPrefix(Schema schema) {
        return switch (schema.getType()) {
            // A boolean is exactly 00 or 01, so FF differs on the first byte.
            case BOOLEAN -> new Prefix(1, true);
            // String/bytes lengths and enum/union indexes are non-negative, four-byte big-endian ints.
            // Their first byte is at most 7F, so FF differs before any content or union branch is read.
            case STRING, BYTES, ENUM, UNION -> new Prefix(1, true);
            // Counts are non-negative ints encoded as unsigned varints: at most five bytes,
            // with the continuation bit clear in the last byte. Five FF bytes are therefore impossible.
            // Shorter counts differ earlier (including 00 for empty collections), so probing stops
            // before consuming items or reading past the value, even when the collection is empty.
            case ARRAY, MAP -> new Prefix(5, true);
            // Null emits no bytes: it cannot distinguish a frame and does not advance the offset.
            case NULL -> new Prefix(0, false);
            // These fixed-width fields can contain an all-FF sequence of their entire width.
            // They provide no distinguishing byte; skip their width and inspect the following field.
            case INT, FLOAT -> new Prefix(4, false);
            case LONG, DOUBLE -> new Prefix(8, false);
            // Fixed is arbitrary bytes with no length header, so the same rule applies.
            case FIXED -> new Prefix(Math.min(schema.getFixedSize(), MAX_AUTOMATIC_MAGIC_BYTES + 1), false);
            case RECORD -> {
                // Records have no header: fields are concatenated, including nested record fields.
                // Add preceding fixed widths to the first distinguishing field's prefix length.
                // For example, int + boolean needs five FF bytes: four may match the int,
                // but the fifth must differ from the boolean. The preceding fields always exist,
                // so reaching that distinguishing byte cannot over-read a valid legacy value.
                int offset = 0;
                boolean excludes = false;
                for (Schema.Field field : schema.getFields()) {
                    Prefix fieldPrefix = automaticPrefix(field.schema());
                    offset = Math.min(MAX_AUTOMATIC_MAGIC_BYTES + 1, offset + fieldPrefix.bytes());
                    excludes = fieldPrefix.isSafeMagic();
                    if (excludes || offset > MAX_AUTOMATIC_MAGIC_BYTES) break;
                }
                yield new Prefix(offset, excludes);
            }
        };
    }

    /** Conservative minimum size, capped at the probe length; recursion uncertainty keeps byte probing. */
    static boolean canBeShorterThanMagic(Schema schema, int magicLength) {
        return minimumBytes(schema, magicLength, 0) < magicLength;
    }

    private static int minimumBytes(Schema schema, int limit, int depth) {
        if (limit == 0 || depth > 128) return 0;
        return switch (schema.getType()) {
            case NULL -> 0;
            case BOOLEAN, ARRAY, MAP -> Math.min(1, limit);
            case INT, FLOAT, ENUM, STRING, BYTES -> Math.min(4, limit);
            case LONG, DOUBLE -> Math.min(8, limit);
            case FIXED -> Math.min(schema.getFixedSize(), limit);
            case UNION -> {
                int branchMinimum = limit;
                for (Schema branch : schema.getTypes()) {
                    branchMinimum = Math.min(branchMinimum, minimumBytes(branch, limit, depth + 1));
                }
                yield (int) Math.min(limit, 4L + branchMinimum);
            }
            case RECORD -> {
                int size = 0;
                for (Schema.Field field : schema.getFields()) {
                    size += minimumBytes(field.schema(), limit - size, depth + 1);
                    if (size == limit) break;
                }
                yield size;
            }
        };
    }

    static String hex(byte[] bytes) {
        return "0x" + HexFormat.of().withUpperCase().formatHex(bytes);
    }

    private sealed interface Item permits Encode, Content, Items, Terminator { }

    private record Encode(Schema schema) implements Item { }

    private record Content(int remaining, boolean utf8) implements Item { }

    /** One more collection element is possible, followed by a terminator or further elements. */
    private record Items(Schema element, boolean map, int previousPosition) implements Item { }

    private record Terminator() implements Item { }

    private record Pending(Item head, Pending tail) { }

    private static final class Walker {
        private final byte[] magic;
        private int budget = STEP_BUDGET;
        private boolean collisionPossible;
        private boolean overReadPossible;

        private Walker(byte[] magic) {
            this.magic = magic;
        }

        private MagicSafety run(Schema schema) {
            walk(new Pending(new Encode(schema), null), 0);
            return new MagicSafety(collisionPossible, overReadPossible);
        }

        private void walk(Pending pending, int position) {
            if (budget-- <= 0) {
                collisionPossible = true;
                overReadPossible = true;
                return;
            }
            if (position >= magic.length) {
                collisionPossible = true;
                return;
            }
            if (pending == null) {
                overReadPossible = true;
                return;
            }
            Pending rest = pending.tail();
            Item item = pending.head();
            if (item instanceof Encode encode) {
                encode(encode.schema(), rest, position);
            } else if (item instanceof Content content) {
                content(content, rest, position);
            } else if (item instanceof Items items) {
                items(items, rest, position);
            } else if (magic[position] == 0) {
                walk(rest, position + 1);
            }
        }

        private void encode(Schema schema, Pending rest, int position) {
            switch (schema.getType()) {
                case NULL -> walk(rest, position);
                case BOOLEAN -> {
                    if ((magic[position] & 0xff) <= 1) walk(rest, position + 1);
                }
                case INT, FLOAT -> raw(4, rest, position);
                case LONG, DOUBLE -> raw(8, rest, position);
                case FIXED -> raw(schema.getFixedSize(), rest, position);
                case STRING -> lengthPrefixed(true, rest, position);
                case BYTES -> lengthPrefixed(false, rest, position);
                case ENUM -> index(schema.getEnumSymbols().size(), null, rest, position);
                case UNION -> index(schema.getTypes().size(), schema.getTypes(), rest, position);
                case ARRAY -> collection(schema.getElementType(), false, rest, position);
                case MAP -> collection(schema.getValueType(), true, rest, position);
                case RECORD -> {
                    Pending next = rest;
                    List<Schema.Field> fields = schema.getFields();
                    for (int index = fields.size() - 1; index >= 0; index--) {
                        next = new Pending(new Encode(fields.get(index).schema()), next);
                    }
                    walk(next, position);
                }
            }
        }

        /** Bytes whose value is unconstrained, such as a two's-complement int or IEEE double. */
        private void raw(int length, Pending rest, int position) {
            if (position + length > magic.length) {
                collisionPossible = true;
                return;
            }
            walk(rest, position + length);
        }

        /** A 4-byte big-endian index below {@code count}; for a union the selected branch follows. */
        private void index(int count, List<Schema> branches, Pending rest, int position) {
            int available = Math.min(4, magic.length - position);
            long smallestValue = 0;
            for (int offset = 0; offset < 4; offset++) {
                int next = offset < available ? magic[position + offset] & 0xff : 0;
                smallestValue = (smallestValue << 8) | next;
            }
            if (smallestValue >= count) return;
            if (available < 4) {
                collisionPossible = true;
                return;
            }
            int value = (int) smallestValue;
            Pending next = branches == null ? rest : new Pending(new Encode(branches.get(value)), rest);
            walk(next, position + 4);
        }

        /** A 4-byte big-endian non-negative length followed by that many content bytes. */
        private void lengthPrefixed(boolean utf8, Pending rest, int position) {
            if ((magic[position] & 0xff) > 0x7f) return;
            if (position + 4 > magic.length) {
                collisionPossible = true;
                return;
            }
            int length = 0;
            for (int offset = 0; offset < 4; offset++) {
                length = (length << 8) | (magic[position + offset] & 0xff);
            }
            walk(new Pending(new Content(length, utf8), rest), position + 4);
        }

        private void content(Content content, Pending rest, int position) {
            int available = magic.length - position;
            int checked = Math.min(content.remaining(), available);
            if (content.utf8()) {
                for (int offset = 0; offset < checked; offset++) {
                    int next = magic[position + offset] & 0xff;
                    // 0xC0, 0xC1 and 0xF5..0xFF never occur in UTF-8.
                    if (next == 0xc0 || next == 0xc1 || next >= 0xf5) return;
                }
            }
            if (content.remaining() > available) {
                collisionPossible = true;
                return;
            }
            walk(rest, position + content.remaining());
        }

        /** Either a single zero byte for an empty collection, or a varint count, the items and a zero byte. */
        private void collection(Schema element, boolean map, Pending rest, int position) {
            if (magic[position] == 0) walk(rest, position + 1);
            long count = 0;
            int offset = 0;
            while (true) {
                if (position + offset >= magic.length) {
                    collisionPossible = true;
                    return;
                }
                int next = magic[position + offset] & 0xff;
                if (offset > 0 && next == 0) return; // writeVarLongCount never emits a trailing zero byte
                count |= (long) (next & 0x7f) << (7 * offset);
                offset++;
                if ((next & 0x80) == 0) break;
                if (offset == 5) return; // item counts are ints, so at most five varint bytes
            }
            if (count < 1 || count > Integer.MAX_VALUE) return;
            int afterCount = position + offset;
            walk(elementItems(element, map, new Pending(new Items(element, map, afterCount), rest)), afterCount);
        }

        private void items(Items items, Pending rest, int position) {
            Pending terminated = new Pending(new Terminator(), rest);
            walk(terminated, position);
            // An element that consumed nothing cannot be repeated meaningfully, so stop the loop there.
            if (position == items.previousPosition()) return;
            walk(elementItems(items.element(), items.map(), new Pending(new Items(items.element(), items.map(), position), rest)),
                    position);
        }

        private static Pending elementItems(Schema element, boolean map, Pending tail) {
            Pending value = new Pending(new Encode(element), tail);
            return map ? new Pending(new Encode(STRING_SCHEMA), value) : value;
        }
    }
}
