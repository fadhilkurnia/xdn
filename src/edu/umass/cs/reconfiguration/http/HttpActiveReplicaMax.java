package edu.umass.cs.reconfiguration.http;

import java.util.logging.Logger;

// TODO: iterative steps to implement updated HttpActiveReplica.
//      1. Just a simple optimized proxy.
//      2. Separate the forwarder client into its own class.
//      3. Add workers for batching requests in the frontend side,
//         then make the forwarder client to support batch of requests.
//      4. Pass the batch of requests into app.execute() method, and make the app
//         to use the newly implemented forwarder client.
//      5. Pass the batch of requests into coordinator.coordinateRequest() method,
//         first in EMULATE_UNREPLICATED mode, then in REPLICATED mode.
public class HttpActiveReplicaMax {

    private static final Logger logger = Logger.getLogger(HttpActiveReplicaMax.class.getName());
    
}
