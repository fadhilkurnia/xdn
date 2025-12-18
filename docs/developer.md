# Intro Document for XDN Developers

## Formatting XDN Java code
We enforce Google Java Style for everything under `src/edu/umass/cs/xdn` and `test`, and CI will block pushes that are not formatted.

- Run `bin/google-java-format.sh` to format the XDN Java sources and tests in-place. The helper downloads `google-java-format` 1.17.0 into `bin/` on first use.
- Run `bin/google-java-format.sh --check` to verify formatting without changing files (same command used in `.github/workflows/google-java-format.yml`).
- Requirements: `bash`, `curl`, and `java` on your PATH. Override the formatter version by exporting `GOOGLE_JAVA_FORMAT_VERSION` before running the script.

## Running test
The simplest way to run a single unit test is by using IDE, such as IntelliJ, our recommended IDE.
There, ensure to register all the `*.jar` inside the `lib` directory into the Project's Library.
To do so, go to "File" -> "Project Structure", then choose "Libraries" 
in the "Project Settings" sidebar.

We use `ant` to build and run all of our unit test, use the following command to run all the unit tests:
```
ant jar
ant xdn-unit-test-console
```

## Running a specific test from the CLI
Use the JUnit ConsoleLauncher shipped in `lib/` so the same classpath is used as our Ant targets.

1. Clean and compile the project and tests:  
   `ant clean jar xdn-compile-tests`
2. Run a single test method via the `execute` subcommand (example for `testGetReplicaInfoSingleService`):  
   ```bash
   java -cp "lib/junit-platform-console-standalone-1.11.1.jar:build/classes:build/test-classes:lib/*" \
     org.junit.platform.console.ConsoleLauncher execute \
     --select-method edu.umass.cs.xdn.XdnGetReplicaInfoTest#testGetReplicaInfoSingleService \
     --details=verbose
   ```
   Another example for `testTwoPaxosBasedServices` that runs two deterministic services:
   ```bash
   java -cp "lib/junit-platform-console-standalone-1.11.1.jar:build/classes:build/test-classes:lib/*" \
     org.junit.platform.console.ConsoleLauncher execute \
     --select-method edu.umass.cs.xdn.XdnMultiServiceTest#testTwoPaxosBasedServices \
     --details=verbose
   ```

Example output (truncated):
```
Test plan execution started. Number of static tests: 1
├─ JUnit Jupiter
│  └─ XdnGetReplicaInfoTest
│     └─ testGetReplicaInfoSingleService() ✔
Test run finished after 0.5s
[ 1 tests found     ]
[ 1 tests successful]
```

## Running all unit tests from the CLI
Use the same ConsoleLauncher to scan the compiled test classes:

```bash
# Compile app + tests
ant clean jar xdn-compile-tests

# Run everything under test/edu/umass/cs
java -cp "lib/junit-platform-console-standalone-1.11.1.jar:build/classes:build/test-classes:lib/*" \
  org.junit.platform.console.ConsoleLauncher execute \
  --scan-class-path build/test-classes \
  --include-package edu.umass.cs \
  --include-classname '.*Test' \
  --details=summary
```

## Logging, or why we should not use printf()

To printout logging for specific classes, you can specify those classes 
in the `./conf/logging.properties` file. For example
```
...
edu.umass.cs.xdn.XdnGeoDemandProfiler.level=FINEST
edu.umass.cs.xdn.XdnReplicaCoordinator.level=FINEST
edu.umass.cs.reconfiguration.http.HttpActiveReplica.level=FINE
```

## TODO
- [ ] Architecture documentation.
- [ ] Consistency model documentation.
- [ ] Replica coordinator documentation.
- [ ] StateDiff capture documentation.
