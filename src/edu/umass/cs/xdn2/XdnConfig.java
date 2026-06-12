package edu.umass.cs.xdn2;

import edu.umass.cs.xdn2.recorder.RecorderType;
import edu.umass.cs.xdn2.sandbox.SandboxType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * XdnConfig reads XDN-specific configuration from a properties file,
 * independent of GigaPaxos's configuration system.
 *
 * Default location: conf/xdn.properties
 * Override via:    -DxdnConfig=path/to/xdn.properties
 *
 * Required properties:
 *   SANDBOX_TYPE   one of: docker (future: podman, gvisor, firecracker, qemu)
 *   RECORDER_TYPE  one of: rsync, fuselog, fuserust, zip
 *
 * Optional properties (used by DockerSandboxManager.waitUntilReady()):
 *   DEFAULT_HEALTHCHECK_INTERVAL_SECONDS  default: 2
 *   DEFAULT_HEALTHCHECK_TIMEOUT_SECONDS   default: 5
 *   DEFAULT_HEALTHCHECK_RETRIES           default: 30
 *
 * All fields are final — XdnConfig is immutable after construction.
 * Required properties throw immediately if absent.
 * Optional properties fall back to hardcoded defaults if absent.
 */
public class XdnConfig {

    private static final String DEFAULT_CONFIG_PATH  = "conf/xdn.properties";
    private static final String CONFIG_PATH_PROPERTY = "xdnConfig";

    private static final String KEY_SANDBOX_TYPE  = "SANDBOX_TYPE";
    private static final String KEY_RECORDER_TYPE = "RECORDER_TYPE";
    private static final String KEY_HC_INTERVAL   = "DEFAULT_HEALTHCHECK_INTERVAL_SECONDS";
    private static final String KEY_HC_TIMEOUT    = "DEFAULT_HEALTHCHECK_TIMEOUT_SECONDS";
    private static final String KEY_HC_RETRIES    = "DEFAULT_HEALTHCHECK_RETRIES";

    private static final int DEFAULT_HC_INTERVAL = 2;
    private static final int DEFAULT_HC_TIMEOUT  = 5;
    private static final int DEFAULT_HC_RETRIES  = 30;

    private final SandboxType sandboxType;
    private final RecorderType recorderType;
    private final int healthcheckIntervalSeconds;
    private final int healthcheckTimeoutSeconds;
    private final int healthcheckRetries;

    private static final Logger logger = Logger.getLogger(XdnConfig.class.getName());

    /**
     * Loads XDN configuration from the default location (conf/xdn.properties)
     * or from the path specified via -DxdnConfig=...
     */
    public XdnConfig() {
        this(resolveConfigPath());
    }

    /**
     * Loads XDN configuration from the given path.
     *
     * @param configPath absolute or relative path to the xdn.properties file
     * @throws RuntimeException if the file is missing or a required property is absent
     */
    public XdnConfig(String configPath) {
        Properties props = loadProperties(configPath);
        this.sandboxType  = parseSandboxType(props, configPath);
        this.recorderType = parseRecorderType(props, configPath);
        this.healthcheckIntervalSeconds = parseOptionalInt(
                props, KEY_HC_INTERVAL, DEFAULT_HC_INTERVAL);
        this.healthcheckTimeoutSeconds  = parseOptionalInt(
                props, KEY_HC_TIMEOUT, DEFAULT_HC_TIMEOUT);
        this.healthcheckRetries         = parseOptionalInt(
                props, KEY_HC_RETRIES, DEFAULT_HC_RETRIES);
    }

    public SandboxType getSandboxType() {
        return sandboxType;
    }

    public RecorderType getRecorderType() {
        return recorderType;
    }

    public int getHealthcheckIntervalSeconds() {
        return healthcheckIntervalSeconds;
    }

    public int getHealthcheckTimeoutSeconds() {
        return healthcheckTimeoutSeconds;
    }

    public int getHealthcheckRetries() {
        return healthcheckRetries;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static String resolveConfigPath() {
        String override = System.getProperty(CONFIG_PATH_PROPERTY);
        if (override != null && !override.isBlank()) {
            return override;
        }
        return DEFAULT_CONFIG_PATH;
    }

    private static Properties loadProperties(String configPath) {
        File file = new File(configPath);
        if (!file.exists()) {
            throw new RuntimeException(
                    "XDN config file not found at '" + configPath + "'. " +
                            "Create conf/xdn.properties or specify a path via " +
                            "-D" + CONFIG_PATH_PROPERTY + "=path/to/xdn.properties");
        }
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(file)) {
            props.load(fis);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read XDN config file at '" + configPath + "': " +
                            e.getMessage());
        }
        logger.info("XdnConfig loaded from: " + file.getAbsolutePath());
        return props;
    }

    private static SandboxType parseSandboxType(Properties props, String configPath) {
        String raw = props.getProperty(KEY_SANDBOX_TYPE);
        if (raw == null || raw.isBlank()) {
            throw new RuntimeException(
                    "Missing required property '" + KEY_SANDBOX_TYPE + "' in " + configPath +
                            ". Valid values: " + validSandboxTypes());
        }
        try {
            return SandboxType.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(
                    "Invalid " + KEY_SANDBOX_TYPE + "='" + raw + "' in " + configPath +
                            ". Valid values: " + validSandboxTypes());
        }
    }

    private static RecorderType parseRecorderType(Properties props, String configPath) {
        String raw = props.getProperty(KEY_RECORDER_TYPE);
        if (raw == null || raw.isBlank()) {
            throw new RuntimeException(
                    "Missing required property '" + KEY_RECORDER_TYPE + "' in " + configPath +
                            ". Valid values: " + validRecorderTypes());
        }
        try {
            return RecorderType.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(
                    "Invalid " + KEY_RECORDER_TYPE + "='" + raw + "' in " + configPath +
                            ". Valid values: " + validRecorderTypes());
        }
    }

    private static int parseOptionalInt(Properties props, String key, int defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                    "Invalid value for optional property '" + key + "': '" + raw +
                            "' is not a valid integer");
        }
    }

    private static String validSandboxTypes() {
        StringBuilder sb = new StringBuilder();
        for (SandboxType t : SandboxType.values()) {
            sb.append(t.name().toLowerCase()).append(", ");
        }
        return sb.substring(0, sb.length() - 2);
    }

    private static String validRecorderTypes() {
        StringBuilder sb = new StringBuilder();
        for (RecorderType t : RecorderType.values()) {
            sb.append(t.name().toLowerCase()).append(", ");
        }
        return sb.substring(0, sb.length() - 2);
    }
}