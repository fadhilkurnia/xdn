package edu.umass.cs.xdn.docker;

import edu.umass.cs.xdn.service.ServiceComponent;
import edu.umass.cs.xdn.service.ServiceInstance;
import edu.umass.cs.xdn.utils.Shell;
import edu.umass.cs.xdn.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * DockerComposeManager generates docker-compose YAML files and wraps docker compose CLI calls. It
 * is intentionally minimal to keep behavior aligned with the existing docker run path.
 */
public final class DockerComposeManager {
  private static final String COMPOSE_BASE_DIR = "/tmp/xdn/compose";
  private static final String HEALTHCHECK_INTERVAL = "5s";
  private static final String HEALTHCHECK_TIMEOUT = "3s";
  private static final String HEALTHCHECK_RETRIES = "20";
  private static final String HEALTHCHECK_START_PERIOD = "10s";

  private DockerComposeManager() {}

  /** Builds a deterministic compose file path for a service epoch. */
  public static String buildComposeFilePath(String nodeId, String serviceName, int epoch) {
    return String.format(
        "%s/%s/%s/e%d/docker-compose.yml", COMPOSE_BASE_DIR, nodeId, serviceName, epoch);
  }

  /** Builds a deterministic compose project name that is safe for docker compose. */
  public static String buildComposeProjectName(String nodeId, String serviceName, int epoch) {
    String raw =
        String.format("xdn-%s-%s-e%d", nodeId, serviceName, epoch).toLowerCase(Locale.ROOT);
    return raw.replaceAll("[^a-z0-9_-]", "_");
  }

  /**
   * Generates a docker-compose YAML that mirrors the current docker run behavior. Component order
   * is preserved via depends_on to keep startup ordering stable.
   */
  public static String generateComposeFile(
      ServiceInstance service,
      int epoch,
      int allocatedPort,
      String stateDirMountSource,
      String stateDirMountTarget,
      String nodeId) {
    // epoch and nodeId are currently unused but kept for future compose metadata hooks.
    StringBuilder sb = new StringBuilder(1024);
    sb.append("services:\n");

    List<ServiceComponent> components = service.property.getComponents();
    ServiceComponent statefulComponent = service.property.getStatefulComponent();
    String statefulComponentName =
        statefulComponent == null ? null : statefulComponent.getComponentName();
    String statefulHealthcheck =
        statefulComponent == null ? null : statefulComponent.getHealthcheckCommand();
    boolean hasStatefulHealthcheck =
        statefulComponentName != null
            && statefulHealthcheck != null
            && !statefulHealthcheck.isEmpty();
    for (int i = 0; i < components.size(); i++) {
      ServiceComponent component = components.get(i);
      String componentName = component.getComponentName();
      String containerName = service.containerNames.get(i);

      sb.append("  ").append(componentName).append(":\n");
      sb.append("    image: ").append(quoteYamlScalar(component.getImageName())).append("\n");
      sb.append("    container_name: ").append(quoteYamlScalar(containerName)).append("\n");
      sb.append("    hostname: ").append(quoteYamlScalar(componentName)).append("\n");
      sb.append("    restart: unless-stopped\n");

      // Ensure the stateful component starts first; other components can start in parallel.
      if (statefulComponentName != null
          && !statefulComponentName.equals(componentName)
          && components.size() > 1) {
        sb.append("    depends_on:\n");
        if (hasStatefulHealthcheck) {
          sb.append("      ").append(statefulComponentName).append(":\n");
          sb.append("        condition: service_healthy\n");
        } else {
          sb.append("      - ").append(statefulComponentName).append("\n");
        }
      }

      if (component.getHealthcheckCommand() != null
          && !component.getHealthcheckCommand().isEmpty()) {
        sb.append("    healthcheck:\n");
        sb.append("      test: [\"CMD-SHELL\", ")
            .append(quoteYamlScalar(component.getHealthcheckCommand()))
            .append("]\n");
        sb.append("      interval: ").append(HEALTHCHECK_INTERVAL).append("\n");
        sb.append("      timeout: ").append(HEALTHCHECK_TIMEOUT).append("\n");
        sb.append("      retries: ").append(HEALTHCHECK_RETRIES).append("\n");
        sb.append("      start_period: ").append(HEALTHCHECK_START_PERIOD).append("\n");
      }

      boolean hasPublish = false;
      if (component.getEntryPort() != null) {
        int publishedPort = component.getEntryPort();
        int hostPort = component.isEntryComponent() ? allocatedPort : publishedPort;
        sb.append("    ports:\n");
        sb.append("      - \"").append(hostPort).append(":").append(publishedPort).append("\"\n");
        hasPublish = true;
      }

      if (!hasPublish && component.getExposedPort() != null) {
        sb.append("    expose:\n");
        sb.append("      - \"").append(component.getExposedPort()).append("\"\n");
      }

      if (component.getEnvironmentVariables() != null
          && !component.getEnvironmentVariables().isEmpty()) {
        sb.append("    environment:\n");
        for (Map.Entry<String, String> env : component.getEnvironmentVariables().entrySet()) {
          String keyVal = env.getKey() + "=" + env.getValue();
          sb.append("      - ").append(quoteYamlScalar(keyVal)).append("\n");
        }
      }

      boolean hasStatefulMount =
          component.isStateful()
              && stateDirMountSource != null
              && !stateDirMountSource.isEmpty()
              && stateDirMountTarget != null
              && !stateDirMountTarget.isEmpty();
      boolean useArbitraryUser = hasStatefulMount && Utils.getUid() != 0;

      if (hasStatefulMount || useArbitraryUser) {
        sb.append("    volumes:\n");
        if (hasStatefulMount) {
          sb.append("      - type: bind\n");
          sb.append("        source: ").append(quoteYamlScalar(stateDirMountSource)).append("\n");
          sb.append("        target: ").append(quoteYamlScalar(stateDirMountTarget)).append("\n");
        }
        if (useArbitraryUser) {
          sb.append("      - /etc/passwd:/etc/passwd:ro\n");
        }
      }

      if (useArbitraryUser) {
        int uid = Utils.getUid();
        int gid = Utils.getGid();
        sb.append("    user: \"").append(uid).append(":").append(gid).append("\"\n");
        sb.append("    cap_add:\n");
        sb.append("      - SYS_NICE\n");
      }
    }

    sb.append("\nnetworks:\n");
    sb.append("  default:\n");
    // Network names include colons, so quote to avoid YAML parsing ambiguity.
    sb.append("    name: ").append(quoteYamlScalar(service.networkName)).append("\n");
    sb.append("    external: true\n");

    return sb.toString();
  }

  /** Starts a compose project, returning false on any non-zero exit code. */
  public static boolean composeUp(String filePath, String projectName) {
    List<String> commandParts = buildComposeCommand(filePath, projectName, "up");
    commandParts.add("-d");
    commandParts.add("--force-recreate");
    return runComposeCommand(commandParts, false, true);
  }

  /** Stops a compose project; exit code 1 is treated as idempotent success. */
  public static boolean composeStop(String filePath, String projectName) {
    List<String> commandParts = buildComposeCommand(filePath, projectName, "stop");
    return runComposeCommand(commandParts, true, true);
  }

  /** Removes a compose project; exit code 1 is treated as idempotent success. */
  public static boolean composeDown(String filePath, String projectName) {
    List<String> commandParts = buildComposeCommand(filePath, projectName, "down");
    return runComposeCommand(commandParts, true, true);
  }

  private static List<String> buildComposeCommand(
      String filePath, String projectName, String action) {
    List<String> commandParts = new ArrayList<>();
    commandParts.add("docker");
    commandParts.add("compose");
    commandParts.add("-f");
    commandParts.add(filePath);
    commandParts.add("-p");
    commandParts.add(projectName);
    commandParts.add(action);
    return commandParts;
  }

  private static boolean runComposeCommand(
      List<String> commandParts, boolean allowNotFound, boolean isSilent) {
    int exitCode = Shell.runCommand(commandParts, isSilent);
    if (exitCode == 0) {
      return true;
    }
    // docker compose returns exit code 1 when resources are already gone.
    return allowNotFound && exitCode == 1;
  }

  private static String quoteYamlScalar(String value) {
    if (value == null) {
      return "\"\"";
    }
    if (!needsQuoting(value)) {
      return value;
    }
    String escaped = value.replace("\\", "\\\\").replace("\"", "\\\"");
    return "\"" + escaped + "\"";
  }

  private static boolean needsQuoting(String value) {
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      if (Character.isWhitespace(ch) || ch == ':' || ch == '#') {
        return true;
      }
    }
    return false;
  }
}
