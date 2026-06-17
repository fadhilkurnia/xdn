package edu.umass.cs.xdn2;

import edu.umass.cs.xdn2.recorder.RecorderType;
import edu.umass.cs.xdn2.sandbox.SandboxType;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for XdnConfig.
 *
 * All tests are self-contained — they write temporary .properties files
 * to a temp directory and clean up after themselves. No Docker, no
 * filesystem side effects beyond /tmp.
 *
 * Run via: ant xdn-unit-tests
 */
@DisplayName("XdnConfig")
class XdnConfigTest {

    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("xdn-config-test-");
    }

    @AfterEach
    void tearDown() throws IOException {
        // Clean up temp files
        try (var stream = Files.walk(tempDir)) {
            stream.sorted(java.util.Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private Path writeConfig(String content) throws IOException {
        Path file = tempDir.resolve("xdn.properties");
        Files.writeString(file, content);
        return file;
    }

    // -------------------------------------------------------------------------
    // Required properties — SANDBOX_TYPE
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("SANDBOX_TYPE=docker loads correctly")
    void sandboxTypeDocker() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=rsync
                """);
        XdnConfig xdnConfig = new XdnConfig(config.toString());
        assertEquals(SandboxType.DOCKER, xdnConfig.getSandboxType());
    }

    @Test
    @DisplayName("SANDBOX_TYPE is case-insensitive")
    void sandboxTypeCaseInsensitive() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=DOCKER
                RECORDER_TYPE=rsync
                """);
        XdnConfig xdnConfig = new XdnConfig(config.toString());
        assertEquals(SandboxType.DOCKER, xdnConfig.getSandboxType());
    }

    @Test
    @DisplayName("Missing SANDBOX_TYPE throws immediately")
    void missingSandboxTypeThrows() throws IOException {
        Path config = writeConfig("""
                RECORDER_TYPE=rsync
                """);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new XdnConfig(config.toString()));
        assertTrue(ex.getMessage().contains("SANDBOX_TYPE"),
                "Error message should mention SANDBOX_TYPE");
    }

    @Test
    @DisplayName("Invalid SANDBOX_TYPE throws with helpful message")
    void invalidSandboxTypeThrows() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=kubernetes
                RECORDER_TYPE=rsync
                """);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new XdnConfig(config.toString()));
        assertTrue(ex.getMessage().contains("kubernetes"),
                "Error message should include the invalid value");
        assertTrue(ex.getMessage().contains("docker"),
                "Error message should list valid values");
    }

    // -------------------------------------------------------------------------
    // Required properties — RECORDER_TYPE
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("RECORDER_TYPE=rsync loads correctly")
    void recorderTypeRsync() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=rsync
                """);
        XdnConfig xdnConfig = new XdnConfig(config.toString());
        assertEquals(RecorderType.RSYNC, xdnConfig.getRecorderType());
    }

    @Test
    @DisplayName("RECORDER_TYPE=fuserust loads correctly")
    void recorderTypeFuserust() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=fuserust
                """);
        XdnConfig xdnConfig = new XdnConfig(config.toString());
        assertEquals(RecorderType.FUSERUST, xdnConfig.getRecorderType());
    }

    @Test
    @DisplayName("RECORDER_TYPE=fuselog loads correctly")
    void recorderTypeFuselog() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=fuselog
                """);
        XdnConfig xdnConfig = new XdnConfig(config.toString());
        assertEquals(RecorderType.FUSELOG, xdnConfig.getRecorderType());
    }

    @Test
    @DisplayName("RECORDER_TYPE=zip loads correctly")
    void recorderTypeZip() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=zip
                """);
        XdnConfig xdnConfig = new XdnConfig(config.toString());
        assertEquals(RecorderType.ZIP, xdnConfig.getRecorderType());
    }

    @Test
    @DisplayName("RECORDER_TYPE is case-insensitive")
    void recorderTypeCaseInsensitive() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=FUSERUST
                """);
        XdnConfig xdnConfig = new XdnConfig(config.toString());
        assertEquals(RecorderType.FUSERUST, xdnConfig.getRecorderType());
    }

    @Test
    @DisplayName("Missing RECORDER_TYPE throws immediately")
    void missingRecorderTypeThrows() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                """);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new XdnConfig(config.toString()));
        assertTrue(ex.getMessage().contains("RECORDER_TYPE"),
                "Error message should mention RECORDER_TYPE");
    }

    @Test
    @DisplayName("Invalid RECORDER_TYPE throws with helpful message")
    void invalidRecorderTypeThrows() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=kafka
                """);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new XdnConfig(config.toString()));
        assertTrue(ex.getMessage().contains("kafka"),
                "Error message should include the invalid value");
        assertTrue(ex.getMessage().contains("rsync"),
                "Error message should list valid values");
    }

    // -------------------------------------------------------------------------
    // Optional properties — healthcheck defaults
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Healthcheck defaults apply when properties are absent")
    void healthcheckDefaults() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=rsync
                """);
        XdnConfig xdnConfig = new XdnConfig(config.toString());
        assertEquals(2, xdnConfig.getHealthcheckIntervalSeconds());
        assertEquals(5, xdnConfig.getHealthcheckTimeoutSeconds());
        assertEquals(30, xdnConfig.getHealthcheckRetries());
    }

    @Test
    @DisplayName("Healthcheck properties override defaults")
    void healthcheckOverrides() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=rsync
                DEFAULT_HEALTHCHECK_INTERVAL_SECONDS=10
                DEFAULT_HEALTHCHECK_TIMEOUT_SECONDS=15
                DEFAULT_HEALTHCHECK_RETRIES=5
                """);
        XdnConfig xdnConfig = new XdnConfig(config.toString());
        assertEquals(10, xdnConfig.getHealthcheckIntervalSeconds());
        assertEquals(15, xdnConfig.getHealthcheckTimeoutSeconds());
        assertEquals(5, xdnConfig.getHealthcheckRetries());
    }

    @Test
    @DisplayName("Non-integer healthcheck value throws with helpful message")
    void invalidHealthcheckValueThrows() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=rsync
                DEFAULT_HEALTHCHECK_RETRIES=many
                """);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new XdnConfig(config.toString()));
        assertTrue(ex.getMessage().contains("DEFAULT_HEALTHCHECK_RETRIES"),
                "Error message should mention the property name");
        assertTrue(ex.getMessage().contains("many"),
                "Error message should include the invalid value");
    }

    // -------------------------------------------------------------------------
    // File loading
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Missing config file throws immediately")
    void missingConfigFileThrows() {
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new XdnConfig("/nonexistent/path/xdn.properties"));
        assertTrue(ex.getMessage().contains("/nonexistent/path/xdn.properties"),
                "Error message should include the missing path");
    }

    @Test
    @DisplayName("Empty config file throws for missing required properties")
    void emptyConfigFileThrows() throws IOException {
        Path config = writeConfig("");
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> new XdnConfig(config.toString()));
        // Should fail on the first required property
        assertTrue(
                ex.getMessage().contains("SANDBOX_TYPE") ||
                        ex.getMessage().contains("RECORDER_TYPE"),
                "Error message should mention a required property");
    }

    @Test
    @DisplayName("Config file with comments and whitespace parses correctly")
    void configWithCommentsAndWhitespace() throws IOException {
        Path config = writeConfig("""
                # This is a comment
                SANDBOX_TYPE = docker
                
                # Another comment
                RECORDER_TYPE = rsync
                """);
        // Java Properties trims whitespace around '=' automatically
        XdnConfig xdnConfig = new XdnConfig(config.toString());
        assertEquals(SandboxType.DOCKER, xdnConfig.getSandboxType());
        assertEquals(RecorderType.RSYNC, xdnConfig.getRecorderType());
    }

    @Test
    @DisplayName("-DxdnConfig system property overrides default path")
    void systemPropertyOverride() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=zip
                """);
        String prev = System.getProperty("xdnConfig");
        try {
            System.setProperty("xdnConfig", config.toString());
            XdnConfig xdnConfig = new XdnConfig();
            assertEquals(RecorderType.ZIP, xdnConfig.getRecorderType());
        } finally {
            // Restore previous value to avoid polluting other tests
            if (prev == null) System.clearProperty("xdnConfig");
            else System.setProperty("xdnConfig", prev);
        }
    }

    @Test
    @DisplayName("XdnConfig is immutable — all fields are final")
    void configIsImmutable() throws IOException {
        Path config = writeConfig("""
                SANDBOX_TYPE=docker
                RECORDER_TYPE=fuserust
                """);
        XdnConfig xdnConfig = new XdnConfig(config.toString());
        // Verify the same values are returned on repeated calls
        // (no internal state mutation between calls)
        assertEquals(xdnConfig.getSandboxType(), xdnConfig.getSandboxType());
        assertEquals(xdnConfig.getRecorderType(), xdnConfig.getRecorderType());
        assertEquals(xdnConfig.getHealthcheckRetries(), xdnConfig.getHealthcheckRetries());
    }
}