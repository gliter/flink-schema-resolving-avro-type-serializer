import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.bundling.Zip
import org.gradle.jvm.toolchain.JavaLanguageVersion
import java.io.File

plugins {
    `java-library`
    `maven-publish`
    signing
}

group = providers.gradleProperty("group").get()

val javaVersion = providers.gradleProperty("javaVersion").map(String::toInt).getOrElse(17)
val flinkVersion = providers.gradleProperty("flinkVersion").getOrElse("2.3.0")
val libraryVersion = providers.gradleProperty("libraryVersion").getOrElse("0.1.0")
version = "$libraryVersion-$flinkVersion"

java {
    toolchain.languageVersion = JavaLanguageVersion.of(javaVersion)
    withSourcesJar()
    withJavadocJar()
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    // Flink supplies these at runtime; consumers choose the matching Flink distribution.
    compileOnly("org.apache.flink:flink-core:$flinkVersion")
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-avro:$flinkVersion")
    compileOnly("org.slf4j:slf4j-api:2.0.17")

    // Avro remains part of the public serializer API.
    api("org.apache.avro:avro:1.12.0")
    // Use Schema Registry's client directly; the application supplies the client at runtime.
    compileOnly("io.confluent:kafka-schema-registry-client:8.1.1")

    testImplementation("org.apache.flink:flink-core:$flinkVersion")
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-avro:$flinkVersion")
    testImplementation("org.apache.flink:flink-clients:$flinkVersion")
    testImplementation("org.apache.flink:flink-statebackend-rocksdb:$flinkVersion")
    testImplementation("org.apache.flink:flink-test-utils-junit:$flinkVersion")
    testImplementation("org.awaitility:awaitility:4.2.2")
    testImplementation("org.apache.avro:avro-compiler:1.12.0")
    testImplementation("io.confluent:kafka-schema-registry-client:8.1.1")
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("uk.org.webcompere:system-stubs-jupiter:2.1.8")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation(platform("org.apache.logging.log4j:log4j-bom:2.26.1"))

    testRuntimeOnly("org.apache.logging.log4j:log4j-api")
    testRuntimeOnly("org.apache.logging.log4j:log4j-core")

    // Only if Log4j is your SLF4J implementation:
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl")
}

configurations.configureEach {
    resolutionStrategy.capabilitiesResolution.withCapability("org.lz4", "lz4-java") {
        select("org.lz4:lz4-java:1.8.0")
    }
}

tasks.withType<Test>().configureEach {
    val isWindows = System.getProperty("os.name").startsWith("Windows", ignoreCase = true)
    val systemTempRoot = File(System.getProperty("java.io.tmpdir")).toPath().root?.toFile()
        ?: layout.buildDirectory.get().asFile
    val flinkTempDirectory = if (isWindows) File(systemTempRoot, "f")
        else layout.buildDirectory.dir("test-tmp/flink").get().asFile
    val rocksDbDirectory = if (isWindows) File(systemTempRoot, "r")
        else layout.buildDirectory.dir("test-tmp/rocksdb").get().asFile

    useJUnitPlatform()
    include("**/*Test.class", "**/*IT.class")
    testLogging { exceptionFormat = TestExceptionFormat.FULL }
    systemProperty("flink.test.flink-tmp-dir", flinkTempDirectory.absolutePath)
    systemProperty("flink.test.rocksdb-local-dir", rocksDbDirectory.absolutePath)
    systemProperty("flink.test.ignore-minicluster-directory-cleanup-failure", isWindows.toString())
    environment("FLINK_CONF_DIR", layout.projectDirectory.dir("src/test/resources/flink-conf").asFile.absolutePath)
    doFirst {
        listOf(flinkTempDirectory, rocksDbDirectory).forEach(File::mkdirs)
    }
}

tasks.javadoc { options.encoding = "UTF-8" }

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "schema-resolving-avro-serializer"
            from(components["java"])
            pom {
                name.set("Schema Resolving Avro Serializer for Flink")
                description.set("Apache Flink TypeSerializer for lazy Avro schema resolution and state migration.")
                url.set("https://github.com/gliter/flink-confluent-avro-type-serializer")
                licenses { license { name.set("The Apache License, Version 2.0"); url.set("https://www.apache.org/licenses/LICENSE-2.0.txt") } }
                developers { developer { id.set("gliter"); name.set("gliter") } }
                scm {
                    connection.set("scm:git:git://github.com/gliter/flink-confluent-avro-type-serializer.git")
                    developerConnection.set("scm:git:ssh://github.com/gliter/flink-confluent-avro-type-serializer.git")
                    url.set("https://github.com/gliter/flink-confluent-avro-type-serializer")
                }
            }
        }
    }
    repositories {
        maven {
            name = "centralBundle"
            url = layout.buildDirectory.dir("central-bundle").get().asFile.toURI()
        }
    }
}

signing {
    val signingKey = providers.gradleProperty("signingKey").orNull ?: System.getenv("SIGNING_KEY")
    val signingPassword = providers.gradleProperty("signingPassword").orNull ?: System.getenv("SIGNING_PASSWORD")
    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications)
    }
}

/** Creates the signed Maven repository ZIP accepted by the Sonatype Central Publisher API. */
val centralBundle = tasks.register<Zip>("centralBundle") {
    dependsOn("publishMavenJavaPublicationToCentralBundleRepository")
    archiveFileName.set("central-bundle.zip")
    destinationDirectory.set(layout.buildDirectory.dir("distributions"))
    from(layout.buildDirectory.dir("central-bundle"))
}
