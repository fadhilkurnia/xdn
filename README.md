# XDN - Replicating Blackbox Stateful Services

## Project structure
```
xdn/
├─ bin/                                   // scripts and binaries
├─ conf/                                  // configuration
├─ docs/
├─ eval/
├─ lib/                                   // evaluation scripts and results
├─ services/                              // example stateful services
├─ src/
│  ├─ edu.umass.cs
│  │  ├─ reconfigurator/
│  │  │  ├─ ReconfigurableNode.java       // the main() method for a Node
│  │  │  │  ...
├─ xdn-cli/                               // xdn cli for developers / service owner
├─ xdn-dns/                               // nameserver for xdn
├─ tests/
├─ build.xml
├─ LICENSE.txt
├─ README.md
```

## System requirements
We developed and tested XDN on rs630 machines in the CloudLab@UMassß, generally 
with the following system requirements:
- Linux 5.15+ with x86-64 architecture, or MacOS with arm64 architecture.
- libfuse3, or rsync.
- Java 21+
- docker 26+, accessible without `sudo`

<a name="getting-started"></a>
## Getting started

1. Pull XDN source code:
    ```
   git pull https://github.com/fadhilkurnia/xdn.git
   cd xdn
    ```
2. Compile XDN source code:
    ```
   ./bin/build_xdn_jar.sh
    ```
3. Compile XDN cli tool `xdn` for app developer:
    ```
   ./bin/build_xdn_cli.sh
    ```
   Make sure to add XDN's `bin` directory into your `$PATH` so you can access `xdn` from anywhere in your machine. 

## [Temporarily Unavailable] Deployment with existing provider
We have prepared a ready-to-use XDN provider at `xdnapp.com`. 
To deploy a stateful service with this existing provider, simply follow the steps below.

1. Configure our `xdn` cli to use the provider, by setting the env variable.
   ```bash
   export XDN_CONTROL_PLANE=cp.xdnapp.com
   ```
2. Deploy your stateful service.
   ```bash
   xdn launch bookcatalog --image=fadhilkurnia/xdn-bookcatalog --state=/app/data/ --deterministic=true
   ```

   Which will result in the following output, if success:
   ```
   Launching tpcc-web service with the following configuration: 
   docker image  : fadhilkurnia/xdn-bookcatalog
   http port     : 80
   consistency   : linearizability
   deterministic : true
   state dir     : /app/data/
   
   The service is successfully launched 🎉🚀
   Access your service at the following permanent URL:
      > http://bookcatalog.xdnapp.com/
   
   
   Retrieve the service's replica locations with this command:
      xdn service info bookcatalog
   Destroy the replicated service with this command:
      xdn service destroy bookcatalog
   ```

   > Note: change `bookcatalog` with another name if there is already existing service with that name.

3. Access your stateful service with the provided url in your browser.
   Alternatively, you can get the deployment info using the command below.
   ```bash
   xdn service info bookcatalog
   ```
4. Once done, remove your service.
   ```bash
   xdn service destroy bookcatalog
   ```

## Deployment in local machine

1. Start the control plane (i.e., Reconfigurator/RC) and the edge servers (i.e., ActiveReplica/AR), 
   using the default configuration.
   ```bash
   ./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.local.properties start all
   ```
   Which will start these 4 local servers:
    - 1 Reconfigurator at localhost:3000.
    - 3 ActiveReplicas at localhost:2000, localhost:2001, and localhost:2002.

2. Set `localhost` as your control plane host.
   ```bash
   export XDN_CONTROL_PLANE=localhost
   ```

3. With `xdn` command line, launch a containerized stateful service with `bookcatalog` as the name:
   ```bash
   xdn launch bookcatalog --image=fadhilkurnia/xdn-bookcatalog --state=/app/data/ --deterministic=true
   ```
   Which will result in the following output, if success:
   ```
   Launching tpcc-web service with the following configuration:
     docker image  : bookcatalog
     http port     : 80
     consistency   : linearizability
     deterministic : true
     state dir     : /app/data/
   
   The service is successfully launched 🎉🚀
   
   Retrieve the service's replica locations with this command:
     xdn service info bookcatalog
   Destroy the replicated service with this command:
     xdn service destroy bookcatalog
   ```

   We can also verify that the replicated services run in the background using `docker ps`.

4. Access the replicated services in the active replicas with curl. Note that we need to specify
   the service name (`bookcatalog`) in the header with `XDN` as the key.
   ```
   # access from the first replica:
   curl -v http://localhost:2300/ -H "XDN: bookcatalog"
   
   # access from the second replica:
   curl -v http://localhost:2301/ -H "XDN: bookcatalog"
   
   # access from the third replica:
   curl -v http://localhost:2302/ -H "XDN: bookcatalog"
   ```

5. (Optional) Update the local host file (`/etc/hosts`), 
   so we can access the web service via `xdn.io` domain:
   ```
   # /etc/hosts file
    127.0.0.1       bookcatalog.ar0.xdn.io
    127.0.0.1       bookcatalog.ar1.xdn.io
    127.0.0.1       bookcatalog.ar2.xdn.io
   ```
   Then you can access the replicated web service with those host, without XDN custom header:
   ```
   curl -v http://bookcatalog.xdn.io:2300/
   curl -v http://bookcatalog.xdn.io:2301/
   curl -v http://bookcatalog.xdn.io:2301/
   ```

> To stop xdn, we need to stop the Reconfigurator and ActiveReplicas and clean all the state:
> ```bash
>  sudo ./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.local.properties forceclear all
>  sudo rm -rf /tmp/gigapaxos
>  sudo rm -rf /tmp/xdn
> ```

## Deployment as XDN provider (TBD)

Assuming we have 4 machines with the following IP address:
- `10.10.1.1` for the first active replica.
- `10.10.1.2` for the second active replica.
- `10.10.1.3` for the third active replica.
- `10.10.1.5` for the reconfiguration.

as what we have specified in the `./conf/gigapaxos.cloudlab.properties`.
If the machines have different IP address, you need to modify the config file.

1. Do all the instructions in the [Getting Started](#getting-started), for each of the machine.

2. Start the server in each machine.
```
# at machine 10.10.1.1:
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.cloudlab.properties start AR1

# at machine 10.10.1.2:
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.cloudlab.properties start AR2

# at machine 10.10.1.3:
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.cloudlab.properties start AR3

# at machine 10.10.1.5:
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.cloudlab.properties start RC1
```

## Deployment in a CloudLab cluster

Assuming we have 5 machines with the following role and IP address:
- `10.10.1.1` for the first active replica.
- `10.10.1.2` for the second active replica.
- `10.10.1.3` for the third active replica.
- `10.10.1.4` for the reconfiguration.
- `10.10.1.5` for the driver machine where we run all the command.

We are using the `xdnd` binary for most of the command here.

1. Initialize the driver machine: `./bin/xdnd init-driver`.
2. Initialize all the remote machines: 
   ```
   ./bin/xdnd dist-init -config=gigapaxos.properties -ssh-key=/ssh/key -username=user
   ```
3. Optionally, initialize observability in all machines:
   ```
   ./bin/xdnd dist-init-observability -config=gigapaxos.properties -ssh-key=/ssh/key -username=user
   ```
4. Start xdn instances in all machines:
   ```
   gpServer.sh -DgigapaxosConfig=gigapaxos.properties start all
   ```