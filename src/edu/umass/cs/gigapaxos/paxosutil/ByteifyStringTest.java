package edu.umass.cs.gigapaxos.paxosutil;

import java.nio.ByteBuffer;

import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.utils.Config;

/**
 * Standalone check that string node IDs byteify and round-trip when
 * BYTEIFY_NON_INT_NODE_IDS is set. Run with the flag on and off:
 *   java -ea -cp jars/*:lib/* -DBYTEIFY_NON_INT_NODE_IDS=true  ...ByteifyStringTest
 *   java -ea -cp jars/*:lib/*                                  ...ByteifyStringTest
 */
public class ByteifyStringTest {
	static int n = 0, fail = 0;

	static void check(String name, boolean ok) {
		n++;
		if (!ok)
			fail++;
		System.out.println((ok ? "  [PASS] " : "  [FAIL] ") + name);
	}

	public static void main(String[] args) throws Exception {
		boolean flag = Config.getGlobalBoolean(PC.BYTEIFY_NON_INT_NODE_IDS);
		System.out.println("BYTEIFY_NON_INT_NODE_IDS = " + flag + "\n");

		// 1. register the real AWS-AZ string node ids
		IntegerMap<String> map = new IntegerMap<String>();
		int cp0 = map.put("cp0"), e1a = map.put("us-east-1a"), e1b = map
				.put("us-east-1b"), e2a = map.put("us-east-2a"), w2a = map
				.put("us-west-2a");
		System.out.println("registered: cp0=" + cp0 + " us-east-1a=" + e1a
				+ " us-east-1b=" + e1b + " us-east-2a=" + e2a + " us-west-2a="
				+ w2a);

		// 2. gate logic + reversible mapping
		check("allInt() is false with string ids", !IntegerMap.allInt());
		check("byteifiable() == flag", IntegerMap.byteifiable() == flag);
		check("reverse map cp0", "cp0".equals(map.get(cp0)));
		check("reverse map us-east-2a", "us-east-2a".equals(map.get(e2a)));
		check("reverse map us-west-2a", "us-west-2a".equals(map.get(w2a)));

		// 3. serialize a RequestPacket whose entry replica is a string node
		RequestPacket req = new RequestPacket(98765L, "geo-demand-payload",
				false);
		req.setEntryReplica(e2a);
		byte[] b = req.toBytes();
		boolean isJSON = b.length > 0 && b[0] == '{';
		System.out.println("\nRequestPacket serialized: " + b.length
				+ " bytes; JSON-form=" + isJSON);

		if (flag) {
			check("byte path taken (not JSON) when flag on", !isJSON);
			RequestPacket back = new RequestPacket(ByteBuffer.wrap(b));
			check("round-trip value preserved",
					"geo-demand-payload".equals(back.requestValue));
			check("round-trip entryReplica int preserved",
					back.getEntryReplica() == e2a);
			check("entryReplica resolves back to us-east-2a",
					"us-east-2a".equals(map.get(back.getEntryReplica())));
		} else {
			check("JSON path taken when flag off", isJSON);
			RequestPacket back = new RequestPacket(new JSONObject(new String(b,
					"ISO-8859-1")));
			check("round-trip value preserved (JSON)",
					"geo-demand-payload".equals(back.requestValue));
		}

		// 5. collision fail-fast: "Aa" and "BB" both have hashCode 2112
		if (flag) {
			IntegerMap<String> m2 = new IntegerMap<String>();
			m2.put("Aa");
			boolean threw = false;
			try {
				m2.put("BB");
			} catch (RuntimeException e) {
				threw = true;
				System.out.println("\ncollision rejected: " + e.getMessage());
			}
			check("hashCode collision (Aa/BB) fails fast when flag on", threw);
		}

		System.out.println("\n"
				+ (fail == 0 ? "ALL " + n + " CHECKS PASSED" : fail + "/" + n
						+ " CHECKS FAILED"));
		System.exit(fail == 0 ? 0 : 1);
	}
}
