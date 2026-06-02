package edu.umass.cs.nio.nioutils;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import edu.umass.cs.nio.MessageNIOTransport;

/**
 * @author arun
 *
 */
public class NIOHeader {
	/**
	 * Size in bytes: two (address, port) pairs, each 16-byte address + 2-byte
	 * port (see {@link AddressCodec}). IPv4 addresses are stored IPv4-mapped.
	 */
	public static final int BYTES = 2 * AddressCodec.SOCKADDR_BYTES;
	/**
	 * 
	 */
	public final InetSocketAddress sndr;
	/**
	 * 
	 */
	public final InetSocketAddress rcvr;
	
	/**
	 * Character set used by NIO for encoding bytes to String and back.
	 */
	public static final String CHARSET = MessageNIOTransport.NIO_CHARSET_ENCODING;

	/**
	 * @param sndr
	 * @param rcvr
	 */
	public NIOHeader(InetSocketAddress sndr, InetSocketAddress rcvr) {
		this.sndr = sndr;
		this.rcvr = rcvr;
	}
	
	/**
	 * @param bytes
	 * @return NIOHeader constructed from the first {@link #BYTES} bytes.
	 * @throws UnknownHostException
	 */
	public static NIOHeader getNIOHeader(byte[] bytes)
			throws UnknownHostException {
		ByteBuffer bbuf = ByteBuffer.wrap(bytes, 0, BYTES);
		InetSocketAddress sndr = AddressCodec.get(bbuf);
		InetSocketAddress rcvr = AddressCodec.get(bbuf);
		return new NIOHeader(sndr, rcvr);
	}

	public String toString() {
		return sndr + "->" + rcvr;
	}

	/**
	 * @return {@code this} as a {@link #BYTES}-byte array: a 16-byte address +
	 *         2-byte port for the sender, then the same for the receiver.
	 */
	public byte[] toBytes() {
		ByteBuffer bbuf = ByteBuffer.wrap(new byte[BYTES]);
		AddressCodec.put(bbuf, this.sndr);
		AddressCodec.put(bbuf, this.rcvr);
		return bbuf.array();
	}
}
