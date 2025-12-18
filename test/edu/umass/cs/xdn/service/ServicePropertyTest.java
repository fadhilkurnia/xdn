package edu.umass.cs.xdn.service;

import java.util.Objects;
import org.json.JSONException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class ServicePropertyTest {

  @Test
  public void ServicePropertyTest_ParseSingleComponentServiceProperties() {
    String serviceName = "alice-book-catalog";
    String prop =
        String.format(
            """
                {
                  "name": "%s",
                  "image": "bookcatalog",
                  "port": 8000,
                  "state": "/data/",
                  "consistency": "linearizability",
                  "deterministic": true
                }
                """,
            serviceName);
    try {
      ServiceProperty sp = ServiceProperty.createFromJsonString(prop);
      assert Objects.equals(sp.getServiceName(), serviceName);
      assert Objects.equals(sp.getComponents().size(), 1);
      assert Objects.equals(sp.getConsistencyModel(), ConsistencyModel.LINEARIZABILITY);
      assert Objects.equals(sp.isDeterministic(), true);

      ServiceComponent c = sp.getComponents().getFirst();
      assert Objects.equals(c.getComponentName(), serviceName);
      assert Objects.equals(c.getExposedPort(), 8000);
      assert Objects.equals(c.getEntryPort(), 8000);
      assert Objects.equals(c.isStateful(), true);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void ServicePropertyTest_ParseTwoComponentsServiceProperties() {
    String serviceName = "dave-note";
    String prop =
        String.format(
            """
                {
                  "name": "%s",
                  "components": [
                    {
                      "backend": {
                        "image": "note-backend",
                        "expose": 8000,
                        "stateful": true
                      }
                    },
                    {
                      "frontend": {
                        "image": "note-frontend",
                        "port": 8080,
                        "entry": true,
                        "environments": [
                          {
                            "BACKEND_HOST": "localhost:8000"
                          }
                        ]
                      }
                    }
                  ],
                  "deterministic": false,
                  "state": "backend:/app/prisma/",
                  "consistency": "causal"
                }
                """,
            serviceName);
    try {
      ServiceProperty sp = ServiceProperty.createFromJsonString(prop);
      assert Objects.equals(sp.getServiceName(), serviceName);
      assert Objects.equals(sp.getComponents().size(), 2);
      assert Objects.equals(sp.getConsistencyModel(), ConsistencyModel.CAUSAL);
      assert Objects.equals(sp.isDeterministic(), false);

      ServiceComponent c1 = sp.getComponents().get(0);
      ServiceComponent c2 = sp.getComponents().get(1);
      assert c1 != null;
      assert c2 != null;
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void ServicePropertyTest_ToJsonStringSingleComponent() {
    String serviceName = "alice-book-catalog";
    String prop =
        String.format(
            """
                {
                  "image": "bookcatalog",
                  "port": 8000,
                  "name": "%s",
                  "deterministic": true,
                  "state": "/data/",
                  "consistency": "linearizability"
                }
                """,
            serviceName);
    String compactedProp = prop.replaceAll("\\s+", "");
    try {
      ServiceProperty sp = ServiceProperty.createFromJsonString(compactedProp);
      Assert.assertEquals(sp.toJsonString(), compactedProp);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Ignore("Disabled due to ongoing development")
  @Test
  public void ServicePropertyTest_ToJsonStringTwoComponents() {
    throw new RuntimeException("unimplemented");
  }
}
