package edu.umass.cs.xdn;

import edu.umass.cs.xdn.request.XdnHttpRequest;

import java.net.InetSocketAddress;

public final class XdnBatchEvent {
    public XdnHttpRequest request;
    public InetSocketAddress clientAddress;
    public XdnRingBufferBatcher.RequestCompletionHandler completionHandler;

    public void set(XdnHttpRequest req,
                    InetSocketAddress addr,
                    XdnRingBufferBatcher.RequestCompletionHandler handler) {
        this.request = req;
        this.clientAddress = addr;
        this.completionHandler = handler;
    }

    public void clear() {
        this.request = null;
        this.clientAddress = null;
        this.completionHandler = null;
    }
}

