package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Docker-free unit tests for {@link XdnBandwidthProfiler}'s parsers, against fixture text captured
 * from a real {@code ss -tinH} / busybox {@code getent hosts} run inside an alpine container.
 */
public class XdnBwProbeParserTest {

  // Two connections: an IPv4 row and a bracketed IPv4-mapped-IPv6 row, each followed by its
  // tcp_info continuation line, exactly as `ss -tinH` prints them.
  private static final String SS_FIXTURE =
      "ESTAB 0      0               10.0.3.2:52912          10.0.3.4:2380\n"
          + "\t cubic wscale:7,7 rto:201 rtt:0.076/0.015 ato:40 mss:32768 pmtu:65535 rcvmss:536"
          + " advmss:65483 cwnd:10 bytes_sent:84 bytes_acked:85 bytes_received:1024 segs_out:17"
          + " segs_in:16 data_segs_out:14 send 34492631579bps lastsnd:139 rcv_space:65495\n"
          + "ESTAB 0      0      [::ffff:10.0.3.2]:2379  [::ffff:172.17.0.1]:39118\n"
          + "\t cubic wscale:7,7 rto:201 rtt:0.193/0.206 ato:40 mss:32768 pmtu:65535 rcvmss:536"
          + " advmss:65483 cwnd:10 bytes_acked:501 bytes_received:84 segs_out:15 segs_in:17\n"
          + "CLOSE-WAIT 0      0               127.0.0.1:40261          127.0.0.1:9000\n"
          + "\t cubic wscale:7,7 rto:201 bytes_sent:84 bytes_acked:85 bytes_received:1\n";

  private static final String GETENT_FIXTURE =
      "10.0.3.2  replica-0  replica-0\n10.0.3.4  replica-1  replica-1\n";

  @Test
  public void testParseSsOutput() {
    List<XdnBandwidthProfiler.SsConn> conns = XdnBandwidthProfiler.parseSsOutput(SS_FIXTURE);
    assertEquals(3, conns.size());

    XdnBandwidthProfiler.SsConn peerConn = conns.get(0);
    assertEquals("10.0.3.2", peerConn.localAddr());
    assertEquals(52912, peerConn.localPort());
    assertEquals("10.0.3.4", peerConn.peerAddr());
    assertEquals(2380, peerConn.peerPort());
    assertEquals(84, peerConn.tx(), "bytes_acked must be corrected for the SYN sequence byte");
    assertEquals(1024, peerConn.rx());

    XdnBandwidthProfiler.SsConn clientConn = conns.get(1);
    assertEquals("10.0.3.2", clientConn.localAddr(), "v4-mapped v6 addresses must normalize");
    assertEquals("172.17.0.1", clientConn.peerAddr());
    assertEquals(2379, clientConn.localPort());
    assertEquals(500, clientConn.tx());
    assertEquals(84, clientConn.rx());
  }

  @Test
  public void testParseGetentOutput() {
    Map<String, String> map = XdnBandwidthProfiler.parseGetentOutput(GETENT_FIXTURE);
    assertEquals(2, map.size());
    assertEquals("replica-0", map.get("10.0.3.2"));
    assertEquals("replica-1", map.get("10.0.3.4"));
  }

  @Test
  public void testClassifyPeer() {
    Map<String, String> map = Map.of("10.0.3.4", "replica-1");
    int entryPort = 2379;
    assertEquals(
        "replica-1", XdnBandwidthProfiler.classifyPeer("10.0.3.4", 2380, 51000, entryPort, map));
    assertEquals(
        XdnBandwidthProfiler.CLIENT_EDGE,
        XdnBandwidthProfiler.classifyPeer("172.17.0.1", 2379, 39000, entryPort, map),
        "bridge-side peers aggregate as client traffic");
    assertEquals(
        XdnBandwidthProfiler.CLIENT_EDGE,
        XdnBandwidthProfiler.classifyPeer("127.0.0.1", 2379, 36694, entryPort, map),
        "proxy-injected loopback on the entry port's server side is client traffic");
    assertNull(
        XdnBandwidthProfiler.classifyPeer("127.0.0.1", 36694, 2379, entryPort, map),
        "the proxy's mirror row must be skipped to avoid double counting");
    assertNull(
        XdnBandwidthProfiler.classifyPeer("127.0.0.1", 3306, 41000, 80, map),
        "sidecar-to-member loopback off the entry port stays intra-pod");
  }

  @Test
  public void testMalformedLinesAreSkipped() {
    String noise = "garbage line\nESTAB 0 0 broken\n\t bytes_acked:10 bytes_received:5\n";
    assertTrue(XdnBandwidthProfiler.parseSsOutput(noise).isEmpty());
  }
}
