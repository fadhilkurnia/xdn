package edu.umass.cs.reconfiguration.testing;

import java.util.Arrays;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.utils.Config;

/**
 * A small JUnit launcher that injects gigapaxos config overrides via
 * {@link Config#register(String[])} (which takes precedence over default
 * values) BEFORE the target test class is loaded. This lets us run existing
 * integration tests such as {@link TESTReconfigurationClient} against an
 * alternate SQL backend (e.g. SQL_TYPE=EMBEDDED_SQLITE) without supplying a
 * gigapaxosConfig properties file, which those harnesses intentionally forbid.
 *
 * Usage: ConfiguredTestRunner &lt;TestClassFQN&gt; [KEY=VALUE ...]
 */
public class ConfiguredTestRunner {
	/**
	 * @param args
	 *            args[0] = fully-qualified test class name; remaining args are
	 *            KEY=VALUE config overrides.
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws ClassNotFoundException {
		String testClass = args.length > 0 ? args[0]
				: "edu.umass.cs.reconfiguration.testing.TESTReconfigurationClient";
		String[] cfg = args.length > 1 ? Arrays.copyOfRange(args, 1, args.length)
				: new String[0];
		// must happen before the test (and thus the SQL logger classes) load
		Config.register(cfg);
		System.out.println("[ConfiguredTestRunner] test=" + testClass
				+ " overrides=" + Arrays.toString(cfg));

		Result r = JUnitCore.runClasses(Class.forName(testClass));
		for (Failure f : r.getFailures()) {
			System.out.println("FAILURE: " + f);
			f.getException().printStackTrace();
		}
		System.out.println("[ConfiguredTestRunner] Tests run: " + r.getRunCount()
				+ ", Failures: " + r.getFailureCount() + ", Ignored: "
				+ r.getIgnoreCount() + ", Time: " + r.getRunTime() + "ms, success="
				+ r.wasSuccessful());
		System.exit(r.wasSuccessful() ? 0 : 1);
	}
}
