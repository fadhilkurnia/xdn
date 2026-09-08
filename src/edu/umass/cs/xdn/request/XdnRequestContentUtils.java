package edu.umass.cs.xdn.request;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

/**
 * Helpers for retaining/releasing an XdnHttpRequest's underlying Netty ByteBuf across an async
 * execution boundary (e.g. handing a request to an executor thread pool after the originating Netty
 * pipeline has moved on). Without this, the pipeline can recycle the buffer before the async work
 * reads it, producing corrupted or garbage request content under concurrent load.
 */
public final class XdnRequestContentUtils {

  private XdnRequestContentUtils() {}

  public static void retain(Object request) {
    if (request instanceof XdnHttpRequestBatch batch) {
      for (XdnHttpRequest xhr : batch.getRequestList()) {
        retainSingle(xhr);
      }
    } else if (request instanceof XdnHttpRequest xhr) {
      retainSingle(xhr);
    }
  }

  private static void retainSingle(XdnHttpRequest xhr) {
    if (xhr.getHttpRequestContent() != null) {
      ByteBuf content = xhr.getHttpRequestContent().content();
      if (content != null && content.refCnt() > 0) {
        ReferenceCountUtil.retain(content);
      }
    }
  }

  public static void release(Object request) {
    if (request instanceof XdnHttpRequestBatch batch) {
      for (XdnHttpRequest xhr : batch.getRequestList()) {
        releaseSingle(xhr);
      }
    } else if (request instanceof XdnHttpRequest xhr) {
      releaseSingle(xhr);
    }
  }

  private static void releaseSingle(XdnHttpRequest xhr) {
    if (xhr.getHttpRequestContent() != null) {
      ByteBuf content = xhr.getHttpRequestContent().content();
      if (content != null && content.refCnt() > 0) {
        ReferenceCountUtil.release(content);
      }
    }
  }
}
