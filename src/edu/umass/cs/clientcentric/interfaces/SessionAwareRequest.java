package edu.umass.cs.clientcentric.interfaces;

// TODO: XDN config that telling us which part of the request defines session ID
//  e.g., Cookie.sessionToken. Also identifiable request, which specify the requestID
//  (or idempotency-key)
public interface SessionAwareRequest {
    String getSessionID();
    void setSessionID(String sessionID);
}
