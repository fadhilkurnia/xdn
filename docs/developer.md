# Intro Document for XDN Developers

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