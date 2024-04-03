package edu.umass.cs.xdn.service;

import java.util.Map;

/**
 * InitializedServiceProperties contains the property of the service, as well as
 * the property of containers where the service runs.
 */
public class InitializedServiceProperties {

    public final String serviceName;

    /**
     * properties of service as declared by the service owner
     */
    public final ServiceProperties properties;

    public final String networkName;

    /**
     * port assigned by XDN in which the container of the service's entry component
     * listen to any HTTP requests.
     */
    public final int mappedPort;

    /**
     * map containing the name of container for each service's component.
     */
    public final Map<String, String> containerNames;

    /**
     * stateDir is the state directory in the stateful component of this service.
     */
    public final String stateDir;

    public InitializedServiceProperties(ServiceProperties properties, String networkName,
                                        int mappedPort, Map<String, String> containerNames) {
        this.serviceName = properties.getServiceName();
        this.properties = properties;
        this.networkName = networkName;
        this.mappedPort = mappedPort;
        this.containerNames = containerNames;

        String finalStateDir = null;
        if (properties.getStateDirectory() != null && !properties.getStateDirectory().isEmpty()) {
            String[] componentAndStateDir = properties.getStateDirectory().split(":");
            if (componentAndStateDir.length == 1) {
                finalStateDir = componentAndStateDir[0];
            }
            if (componentAndStateDir.length == 2) {
                finalStateDir = componentAndStateDir[1];
            }
        }
        this.stateDir = finalStateDir;
    }
}
