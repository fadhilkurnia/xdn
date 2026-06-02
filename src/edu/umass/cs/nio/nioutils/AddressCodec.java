package edu.umass.cs.nio.nioutils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * Fixed-width on-wire codec for {@link InetSocketAddress} that supports BOTH
 * IPv4 and IPv6.
 *
 * <p>Every address is serialized as a fixed <b>16-byte</b> field followed by a
 * 2-byte port ({@link #SOCKADDR_BYTES} total). IPv4 addresses are stored as
 * their IPv4-mapped IPv6 form ({@code ::ffff:a.b.c.d}); on read,
 * {@link InetAddress#getByAddress(byte[])} returns an {@code Inet4Address} for
 * IPv4-mapped input and an {@code Inet6Address} otherwise, so an IPv4 address
 * round-trips back to an IPv4 address.
 *
 * <p>Keeping the field fixed-width (just wider than the legacy 4 bytes) means
 * all the fixed-offset packet layouts in gigapaxos remain deterministic; only
 * the size constants and offsets change. This is a wire-format change: all
 * nodes in a cluster must run a build that uses this codec.
 *
 * @author arun
 */
public class AddressCodec {

	/** Bytes used for the IP address portion (16 = IPv6 / IPv4-mapped). */
	public static final int ADDRESS_BYTES = 16;

	/** Bytes used for a full address+port pair (16 + 2). */
	public static final int SOCKADDR_BYTES = ADDRESS_BYTES + Short.BYTES;

	/**
	 * Encode {@code addr} (4- or 16-byte) into a 16-byte buffer, using the
	 * IPv4-mapped IPv6 form for IPv4 input.
	 *
	 * @param addr 4- or 16-byte raw address, or {@code null} for all-zeros.
	 * @return a 16-byte array.
	 */
	public static byte[] toSixteen(byte[] addr) {
		if (addr == null)
			return new byte[ADDRESS_BYTES];
		if (addr.length == ADDRESS_BYTES)
			return addr;
		// 4-byte IPv4 -> ::ffff:a.b.c.d
		byte[] mapped = new byte[ADDRESS_BYTES];
		mapped[10] = (byte) 0xff;
		mapped[11] = (byte) 0xff;
		System.arraycopy(addr, 0, mapped, 12, 4);
		return mapped;
	}

	/**
	 * Write {@code sa} as 16 bytes of address + 2 bytes of port into {@code bbuf}.
	 * A {@code null} address/port is written as zeros (port 0 signals "absent"
	 * to {@link #getOrNull(ByteBuffer)}).
	 *
	 * @param bbuf destination buffer (advanced by {@link #SOCKADDR_BYTES}).
	 * @param sa   the address (may be {@code null}).
	 */
	public static void put(ByteBuffer bbuf, InetSocketAddress sa) {
		byte[] raw = (sa != null && sa.getAddress() != null) ? sa.getAddress()
				.getAddress() : null;
		bbuf.put(toSixteen(raw));
		bbuf.putShort(sa != null ? (short) sa.getPort() : 0);
	}

	/**
	 * Read a 16-byte address + 2-byte port, always returning a non-null
	 * {@link InetSocketAddress} (mirrors the legacy {@code NIOHeader} behavior of
	 * always constructing an address).
	 *
	 * @param bbuf source buffer (advanced by {@link #SOCKADDR_BYTES}).
	 * @return the decoded address (never {@code null}).
	 * @throws UnknownHostException if the 16 bytes are not a valid address.
	 */
	public static InetSocketAddress get(ByteBuffer bbuf)
			throws UnknownHostException {
		byte[] a = new byte[ADDRESS_BYTES];
		bbuf.get(a);
		int port = bbuf.getShort() & 0xffff;
		return new InetSocketAddress(InetAddress.getByAddress(a), port);
	}

	/**
	 * Like {@link #get(ByteBuffer)} but returns {@code null} when the port is 0,
	 * matching the "absent address" convention used by {@code RequestPacket}.
	 *
	 * @param bbuf source buffer (advanced by {@link #SOCKADDR_BYTES}).
	 * @return the decoded address, or {@code null} if the port was 0.
	 * @throws UnknownHostException if the 16 bytes are not a valid address.
	 */
	public static InetSocketAddress getOrNull(ByteBuffer bbuf)
			throws UnknownHostException {
		byte[] a = new byte[ADDRESS_BYTES];
		bbuf.get(a);
		int port = bbuf.getShort() & 0xffff;
		return port != 0 ? new InetSocketAddress(InetAddress.getByAddress(a),
				port) : null;
	}
}
