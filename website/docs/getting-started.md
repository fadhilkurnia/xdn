# Getting started with XDN

This page explains how to deploy a blackbox stateful service on an existing XDN provider.

## Install the CLI

Install the `xdn` CLI with a single command — it downloads the right binary for
your OS and architecture, verifies its SHA-256 checksum, and installs it to
`~/.local/bin`:

``` sh
curl -fsSL https://xdn.cs.umass.edu/install | sh
```

Check that it runs:

``` sh
xdn --help
```

??? quote "Other ways to get it"
    Inspect the script first with `curl -fsSL https://xdn.cs.umass.edu/install | less`,
    grab a binary directly from the [releases page](https://github.com/fadhilkurnia/xdn/releases),
    or build from source:
    `git clone https://github.com/fadhilkurnia/xdn && cd xdn && ./bin/build_xdn_cli.sh`.

## Deploy a blackbox service

XDN places your service's replicas at edge locations close to your users —
across AWS Regions and Local Zones worldwide. Here are the available edge
locations:

<iframe class="xdn-edge-map" src="/dashboard/app/edge-locations.html"
        title="XDN edge locations" loading="lazy"></iframe>

<style>
  .xdn-edge-map {
    width: 100%; height: 420px; margin: 4px 0 8px;
    border: 1px solid var(--md-default-fg-color--lightest); border-radius: 6px;
  }
</style>

Finally, launch a blackbox stateful service on XDN. Let's use `bookcatalog` as the service name.
``` sh
xdn launch bookcatalog \
   --image=fadhilkurnia/xdn-bookcatalog \ 
   --port=80 \
   --consistency=linearizable \
   --deterministic=true \
   --state=/app/data/
```
If successful, you will see the following output below, then you can access the stateful service by visiting
http://bookcatalog.xdnapp.com/.
   ```
   Launching bookcatalog service with the following configuration:
     docker image  : fadhilkurnia/xdn-bookcatalog
     http port     : 80
     consistency   : linearizable
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

Let's dechiper what just happened when we deploy a stateful service with the command above.

- **Blackbox Service.** XDN handles arbitrary stateful service, the `--image` specifies the Docker image of the
  containerized service. XDN doesn't need to know how and with what programming language the service was implemented.
- **HTTP Interface.** XDN acts as proxy for all incoming HTTP request, so XDN can coordinate the requests among the 
  replicas. The `--port` option specifies the port where the service listen to incoming HTTP requests. When unspecified,
  XDN asssumes the default HTTP port of 80.
- **Service Properties**. The `--deterministic` option specifies whether the web service is deterministic or not. Other
  than determinism, XDN allows developer to specify other properties of the service and its requests so XDN can use an
  optimized replication protocol, depending on the service's properties.
- **Consistency Model.** The `--consistency` option specifies the consistency model the developer wants for the
  replicated service. The default value is `linearizable`. Check out [this page](flexible-consistency.md) to see how
  to use different consistency model.
- **State Directory.** The `--state` option specifies the directory where the web service stores its state. For example,
  it is commonly `/var/lib/mysql` in MySQL and `/var/lib/pgsql/data` in PostgreSQL. When not specified, XDN will 
  snapshot the entire data in the container `/`. 
- **XDN Provider.** Here, we are using an existing XDN provider, accessible at `xdnapp.com`. You can use another XDN
  Provider using `--control-plane=<control_plane_url>` option. Alternatively, you can be your own XDN Provider! check
  out [this page](become-operator.md).

## Deploy using a service declaration file

Instead of passing each property as a separate CLI flag, you can declare the whole
service in a YAML file and launch it with `--file`, which keeps the service
definition versionable. Here is the `bookcatalog` service from above, written as
`bookcatalog.yaml`:

```yaml
# bookcatalog.yaml
---
name: bookcatalog
image: fadhilkurnia/xdn-bookcatalog
port: 80
consistency: linearizability
deterministic: true
state: /app/data/
```

Then launch it with:
``` bash
xdn launch bookcatalog --file=bookcatalog.yaml
```

This is equivalent to the `xdn launch bookcatalog --image=… --state=…` command shown earlier.

A declaration file really shines when a service is made of several containers — a
frontend, a backend, and a database, for example. See
[Deploy a multi-container service](multi-container.md) for how to declare and launch one.

## Other example services

XDN can replicate _any_ stateful service, as long as it exposes a request-response
HTTP interface and keeps its safety-critical state on disk. Other than the
`fadhilkurnia/xdn-bookcatalog` Docker image that we use previously, we have prepared Docker images for
other stateful services, as can be seen below.

<table>
<tr>
	<th>Docker Image</th>
    <th>Description</th>
    <th>Example Launch Command</th>
</tr>
<tr>
    <td>fadhilkurnia/xdn-bookcatalog</td>
    <td>Book catalog web app, storing updatable list of books. <br>Tech: Go, SQLite.</td>
    <td>
      ``` bash
      xdn launch alice-catalog \
           --image=fadhilkurnia/xdn-bookcatalog \
           --state=/data/ \
           --deterministic
      ```
    </td>
</tr>
<tr>
    <td>fadhilkurnia/xdn-bookcatalog-nd</td>
    <td>Book catalog web app that is non-deterministic because it stores the update timestamp. <br>Tech: Go, SQLite.</td>
    <td>
      ``` bash
      xdn launch bob-catalog \
          --image=fadhilkurnia/xdn-bookcatalog-nd \
          --state=/data/
      ```
    </td>
</tr>
<tr>
    <td>fadhilkurnia/xdn-tpcc</td>
    <td>App for a wholesale parts supplier that owns multiple warehouse, implementing <br>the TPC-C benchmark. <br>Tech: Python, SQLite.</td>
    <td>
      ``` bash
      xdn launch charlie-tpcc \
          --image=fadhilkurnia/xdn-tpcc \
          --state=/app/data/
      ```
    </td>
</tr>
<tr>
    <td>fadhilkurnia/xdn-todo</td>
    <td>Todo application, enabling users to list and modify <br>their todo items. <br>Tech: Go, SQLite.</td>
    <td>
      ``` bash
      xdn launch dave-todo \
          --image=fadhilkurnia/xdn-todo \
          --state=/app/data/
      ```
    </td>
</tr>
<tr>
    <td>fadhilkurnia/xdn-webkv</td>
    <td>Key-value store with a simple REST API <br>(YCSB-style <code>GET/PUT/POST/DELETE /api/kv/:key</code>). <br>Tech: Rust, RocksDB.</td>
    <td>
      ``` bash
      xdn launch mykv \
          --image=fadhilkurnia/xdn-webkv \
          --state=/app/data/ \
          --deterministic
      ```
    </td>
</tr>
<tr>
    <td>fadhilkurnia/xdn-noop</td>
    <td>No-op baseline service: returns <code>200 ok</code> <br>immediately with no state or work. Useful to <br>measure XDN's coordination overhead. <br>Tech: Go.</td>
    <td>
      ``` bash
      xdn launch noop \
          --image=fadhilkurnia/xdn-noop
      ```
    </td>
</tr>
<tr>
    <td>fadhilkurnia/xdn-hotel-reservation</td>
    <td>Hotel reservation app — a consolidated port of <br>DeathStarBench's hotel reservation (frontend + MongoDB). <br>Tech: Go, MongoDB.</td>
    <td>
      Multi-container; launch from a declaration file <br>(see <a href="multi-container.md">Deploy a multi-container service</a>):
      ``` bash
      xdn launch hotel \
          --file=hotel-reservation.yaml
      ```
    </td>
</tr>
<tr>
    <td></td>
    <td>Coming soon: <br>movie review app</td>
    <td></td>
</tr>
<tr>
    <td>fadhilkurnia/xdn-smallbank</td>
    <td>SmallBank OLTP benchmark: savings/checking <br>accounts with money-moving transactions. <br>Tech: Go, SQLite.</td>
    <td>
      ``` bash
      xdn launch bank \
          --image=fadhilkurnia/xdn-smallbank \
          --state=/app/data/ \
          --deterministic
      ```
    </td>
</tr>
<tr>
    <td></td>
    <td>Coming soon: <br>e-commerce</td>
    <td></td>
</tr>
<tr>
    <td></td>
    <td>Coming soon: <br>social network app</td>
    <td></td>
</tr>
<tr>
    <td>fadhilkurnia/xdn-seats</td>
    <td>SEATS airline-ticketing benchmark: airports, <br>flights, customers, and seat reservations. <br>Tech: Go, SQLite.</td>
    <td>
      ``` bash
      xdn launch seats \
          --image=fadhilkurnia/xdn-seats \
          --state=/app/data/ \
          --deterministic
      ```
    </td>
</tr>

</table>
