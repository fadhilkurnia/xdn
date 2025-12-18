package edu.umass.cs.xdn;

import edu.umass.cs.reconfiguration.interfaces.InitialStateValidator;
import edu.umass.cs.xdn.service.ServiceComponent;
import edu.umass.cs.xdn.service.ServiceProperty;
import edu.umass.cs.xdn.utils.Shell;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONException;

public class XdnServiceInitialStateValidator implements InitialStateValidator {

  @Override
  public void validateInitialState(String initialState) throws InvalidInitialStateException {
    if (initialState == null || initialState.isEmpty()) {
      throw new InvalidInitialStateException(
          "Initial state for an XDN service cannot be null or empty.");
    }

    String validInitialStatePrefix = ServiceProperty.XDN_INITIAL_STATE_PREFIX;

    // validate the prefix of the initialState
    if (!initialState.startsWith(validInitialStatePrefix)) {
      throw new InvalidInitialStateException(
          "Invalid prefix for the initial state, expecting " + validInitialStatePrefix);
    }

    // try to decode the initialState (without prefix), containing the service property encoded
    // as JSON data
    initialState = initialState.substring(validInitialStatePrefix.length());
    ServiceProperty property = null;
    try {
      property = ServiceProperty.createFromJsonString(initialState);
    } catch (JSONException e) {
      throw new InvalidInitialStateException(
          "Invalid initial state, expecting valid JSON data. Error: " + e.getMessage());
    } catch (RuntimeException e) {
      throw new InvalidInitialStateException(e.getMessage());
    }

    // try to validate all the provided container image names
    Set<String> containerNames = new HashSet<>();
    for (ServiceComponent c : property.getComponents()) {
      containerNames.add(c.getImageName());
    }
    for (String imageName : containerNames) {
      // First, try to inspect local image, if any.
      String command = String.format("docker image inspect %s", imageName);
      int errCode = Shell.runCommand(command, true);
      if (errCode == 0) {
        continue;
      }

      // Fallback by checking remote image.
      command = String.format("docker manifest inspect --insecure %s", imageName);
      errCode = Shell.runCommand(command, true);
      if (errCode != 0) {
        String exceptionMessage =
            String.format(
                "Unknown container image with name '%s'. Ensure the image is accessible "
                    + "at Docker Hub, our default container registry. An example of "
                    + "container available from Docker Hub is "
                    + "'fadhilkurnia/xdn-bookcatalog'. You can also use container from "
                    + "other registries (e.g., ghcr.io, quay.io, etc).",
                imageName);
        throw new InvalidInitialStateException(exceptionMessage);
      }
    }
  }
}
