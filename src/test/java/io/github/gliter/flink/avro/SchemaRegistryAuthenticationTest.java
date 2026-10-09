package io.github.gliter.flink.avro;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SystemStubsExtension.class)
class SchemaRegistryAuthenticationTest {
    private static final String USER_INFO = "serializer-test:local-only-password";
    private static final String USER_INFO_ENV = "SCHEMA_REGISTRY_TEST_USER_INFO";
    private static final Schema CUSTOMER_SCHEMA = new Schema.Parser().parse("""
            {"type":"record","name":"Customer","namespace":"auth",
             "fields":[{"name":"name","type":"string"}]}
            """);

    @SystemStub
    private final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    void serializerAuthenticatesUsingEnvironmentAndFileSecrets() throws Exception {
        try (AuthenticatedRegistry registry = new AuthenticatedRegistry(CUSTOMER_SCHEMA, USER_INFO)) {
            environmentVariables.set(USER_INFO_ENV, USER_INFO);
            environmentVariables.set("FLINK_CONF_DIR", flinkConfigWithEnvironmentSecret(
                    registry.url(), USER_INFO_ENV).toString());
            byte[] environmentSecretValue = writeAndRead(registry.url());

            Path secretFile = Files.createTempFile("schema-registry-auth", ".secret");
            Files.writeString(secretFile, "  " + USER_INFO + "  \r\n");
            environmentVariables.set("FLINK_CONF_DIR", flinkConfigWithFileSecret(
                    registry.url(), secretFile).toString());
            byte[] fileSecretValue = writeAndRead(registry.url());

            assertTrue(registry.authenticatedRequests().stream()
                    .anyMatch(path -> path.startsWith("POST /subjects/customer-state-subject/versions")));
            assertTrue(registry.authenticatedRequests().stream()
                    .anyMatch(path -> path.startsWith("GET /schemas/ids/1")));
            assertTrue(registry.unauthorizedRequests().isEmpty());
            assertFalse(new String(environmentSecretValue, StandardCharsets.ISO_8859_1).contains(USER_INFO));
            assertFalse(new String(fileSecretValue, StandardCharsets.ISO_8859_1).contains(USER_INFO));
        }
    }

    private static byte[] writeAndRead(String registryUrl) throws Exception {
        Map<String, Object> serializerConfig = Map.of(
                "schema.registry.url", registryUrl,
                "auto.register.schemas", true);
        SchemaResolvingAvroSerializer<GenericRecord> writer = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state-subject", CUSTOMER_SCHEMA, serializerConfig);
        GenericRecord value = new GenericData.Record(CUSTOMER_SCHEMA);
        value.put("name", "Ada");
        DataOutputSerializer output = new DataOutputSerializer(128);
        writer.serialize(value, output);

        SchemaResolvingAvroSerializer<GenericRecord> reader = SchemaResolvingAvroSerializer.forGeneric(
                "customer-state-subject", CUSTOMER_SCHEMA, serializerConfig);
        GenericRecord restored = reader.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
        assertEquals("Ada", restored.get("name").toString());

        DataOutputSerializer snapshot = new DataOutputSerializer(128);
        reader.snapshotConfiguration().writeSnapshot(snapshot);
        assertFalse(new String(snapshot.getCopyOfBuffer(), StandardCharsets.ISO_8859_1).contains(USER_INFO));
        return snapshot.getCopyOfBuffer();
    }

    private static Path flinkConfigWithEnvironmentSecret(String registryUrl, String environmentVariable)
            throws IOException {
        Path directory = Files.createTempDirectory("schema-registry-env-config");
        Files.writeString(directory.resolve("config.yaml"), """
                schema.resolving.avro.schema.registry.url: %s
                schema.resolving.avro.basic.auth.credentials.source: USER_INFO
                schema.resolving.avro.secret.schema.registry.basic.auth.user.info.env: %s
                """.formatted(registryUrl, environmentVariable));
        return directory;
    }

    private static Path flinkConfigWithFileSecret(String registryUrl, Path secretFile) throws IOException {
        Path directory = Files.createTempDirectory("schema-registry-file-config");
        String escapedPath = secretFile.toAbsolutePath().toString()
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
        Files.writeString(directory.resolve("config.yaml"), """
                schema.resolving.avro.schema.registry.url: %s
                schema.resolving.avro.basic.auth.credentials.source: USER_INFO
                schema.resolving.avro.secret.schema.registry.basic.auth.user.info.file: "%s"
                """.formatted(registryUrl, escapedPath));
        return directory;
    }

    private static final class AuthenticatedRegistry implements AutoCloseable {
        private final HttpServer server;
        private final Schema schema;
        private final String expectedAuthorization;
        private final List<String> authenticatedRequests = new CopyOnWriteArrayList<>();
        private final List<String> unauthorizedRequests = new CopyOnWriteArrayList<>();

        private AuthenticatedRegistry(Schema schema, String userInfo) throws IOException {
            this.schema = schema;
            this.expectedAuthorization = "Basic " + Base64.getEncoder().encodeToString(
                    userInfo.getBytes(StandardCharsets.UTF_8));
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        private String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private List<String> authenticatedRequests() {
            return authenticatedRequests;
        }

        private List<String> unauthorizedRequests() {
            return unauthorizedRequests;
        }

        private void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().toString();
            if (!expectedAuthorization.equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                unauthorizedRequests.add(exchange.getRequestMethod() + " " + path);
                exchange.getResponseHeaders().set("WWW-Authenticate", "Basic realm=\"test-registry\"");
                respond(exchange, 401, "{\"error_code\":401,\"message\":\"Unauthorized\"}");
                return;
            }

            authenticatedRequests.add(exchange.getRequestMethod() + " " + path);
            if ("POST".equals(exchange.getRequestMethod()) && path.startsWith("/subjects/")
                    && path.contains("/versions")) {
                exchange.getRequestBody().readAllBytes();
                respond(exchange, 200, "{\"id\":1}");
            } else if ("GET".equals(exchange.getRequestMethod()) && path.startsWith("/schemas/ids/1")) {
                respond(exchange, 200, "{\"schema\":" + jsonString(schema.toString())
                        + ",\"schemaType\":\"AVRO\"}");
            } else {
                respond(exchange, 404, "{\"error_code\":404,\"message\":\"Not found\"}");
            }
        }

        private static String jsonString(String value) {
            return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"")
                    .replace("\r", "\\r").replace("\n", "\\n") + "\"";
        }

        private static void respond(HttpExchange exchange, int status, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (var response = exchange.getResponseBody()) {
                response.write(bytes);
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
