package edu.umass.cs.xdn.request;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;

@Deprecated
public record XDNRequestAndCallback(XdnRequest request, ExecutedCallback callback) {}
