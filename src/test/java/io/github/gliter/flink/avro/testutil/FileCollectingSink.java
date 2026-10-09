package io.github.gliter.flink.avro.testutil;

import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Collects job events in a file visible to both the MiniCluster and the test JVM. */
public final class FileCollectingSink<T> implements SinkFunction<T> {
    private final String outputPath;

    public FileCollectingSink(Path outputPath) {
        this.outputPath = outputPath.toString();
    }

    @Override
    public void invoke(T value, Context context) throws IOException {
        Files.writeString(Path.of(outputPath), value + System.lineSeparator(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
