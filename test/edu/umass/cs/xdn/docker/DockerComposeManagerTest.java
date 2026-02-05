package edu.umass.cs.xdn.docker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.umass.cs.xdn.service.ServiceInstance;
import edu.umass.cs.xdn.service.ServiceProperty;
import edu.umass.cs.xdn.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONException;
import org.junit.jupiter.api.Test;

public class DockerComposeManagerTest {

  @Test
  public void generateComposeFile_WordpressMultiComponent() throws JSONException {
    String serviceName = "wordpress";
    String propJson =
        """
            {
              "name": "wordpress",
              "components": [
                {
                  "database": {
                    "image": "mysql:8.4.0",
                    "expose": 3306,
                    "stateful": true,
                    "healthcheck": {
                      "command": "mysqladmin ping -h localhost -p$MYSQL_ROOT_PASSWORD"
                    },
                    "environments": [
                      { "MYSQL_ROOT_PASSWORD": "supersecret" }
                    ]
                  }
                },
                {
                  "wordpress": {
                    "image": "wordpress:6.5.4-apache",
                    "port": 80,
                    "entry": true,
                    "environments": [
                      { "WORDPRESS_DB_HOST": "database:3306" },
                      { "WORDPRESS_DB_USER": "root" }
                    ]
                  }
                }
              ],
              "deterministic": true,
              "state": "database:/var/lib/mysql/",
              "consistency": "linearizability"
            }
            """;

    ServiceProperty property = ServiceProperty.createFromJsonString(propJson);

    List<String> containerNames = new ArrayList<>();
    containerNames.add("c0.e0.wordpress.ar1.xdn.io");
    containerNames.add("c1.e0.wordpress.ar1.xdn.io");

    ServiceInstance service =
        new ServiceInstance(property, serviceName, "net::ar1:wordpress", containerNames);

    String yaml =
        DockerComposeManager.generateComposeFile(
            service,
            0,
            52341,
            "/tmp/xdn/state/rsync/ar1/mnt/wordpress/e0/",
            "/var/lib/mysql/",
            "ar1");

    // Validate key fields for the database component.
    assertTrue(yaml.contains("database:"), "database service missing");
    assertTrue(
        yaml.contains("container_name: c0.e0.wordpress.ar1.xdn.io"),
        "database container name missing");
    assertTrue(yaml.contains("hostname: database"), "database hostname missing");
    assertTrue(yaml.contains("expose:"), "database expose missing");
    assertTrue(yaml.contains("\"3306\""), "database exposed port missing");
    assertTrue(yaml.contains("healthcheck:"), "database healthcheck missing");
    assertTrue(
        yaml.contains("CMD-SHELL"), "healthcheck command must use CMD-SHELL for env expansion");

    // Validate key fields for the wordpress entry component.
    assertTrue(yaml.contains("wordpress:"), "wordpress service missing");
    assertTrue(
        yaml.contains("container_name: c1.e0.wordpress.ar1.xdn.io"),
        "wordpress container name missing");
    assertTrue(
        yaml.contains("depends_on:\n      database:\n        condition: service_healthy"),
        "depends_on service_healthy missing");
    assertTrue(yaml.contains("\"52341:80\""), "entry port mapping missing");

    // Ensure env values with colons are quoted to keep YAML valid.
    assertTrue(yaml.contains("- \"WORDPRESS_DB_HOST=database:3306\""), "quoted env value missing");

    // Ensure network name is quoted because it contains colons.
    assertTrue(yaml.contains("name: \"net::ar1:wordpress\""), "quoted network name missing");

    // Optional: if running as non-root, ensure user/cap_add are emitted for stateful mounts.
    if (Utils.getUid() != 0) {
      assertTrue(yaml.contains("/etc/passwd:/etc/passwd:ro"), "passwd mount missing");
      assertTrue(yaml.contains("cap_add:"), "cap_add missing");
    }
  }

  @Test
  public void generateComposeFile_BookcatalogSingleComponent() throws JSONException {
    String serviceName = "bookcatalog";
    String propJson =
        """
            {
              "name": "bookcatalog",
              "image": "bookcatalog:latest",
              "port": 8000,
              "state": "/data/",
              "deterministic": true,
              "consistency": "linearizability"
            }
            """;

    ServiceProperty property = ServiceProperty.createFromJsonString(propJson);

    List<String> containerNames = new ArrayList<>();
    containerNames.add("c0.e0.bookcatalog.ar1.xdn.io");

    ServiceInstance service =
        new ServiceInstance(property, serviceName, "net::ar1:bookcatalog", containerNames);

    String yaml =
        DockerComposeManager.generateComposeFile(
            service, 0, 54000, "/tmp/xdn/state/rsync/ar1/mnt/bookcatalog/e0/", "/data/", "ar1");

    // Ensure the entry port is published and no depends_on appears for single component.
    assertTrue(yaml.contains("\"54000:8000\""), "entry port mapping missing");
    assertFalse(yaml.contains("depends_on"), "unexpected depends_on for single component");

    // Published ports should suppress expose in single component services.
    assertFalse(yaml.contains("expose:\n"), "unexpected expose for entry component");
  }
}
