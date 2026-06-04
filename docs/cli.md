# Command-line (`xdn`)

`xdn` is the command-line client for deploying and managing replicated stateful
services. It is a thin wrapper over the [Control-plane HTTP API](HTTP-API.md).

## Pointing at a control plane

Commands talk to the control plane named by `XDN_CONTROL_PLANE` (default
`localhost`, port `3300`):

```bash
export XDN_CONTROL_PLANE=cp.xdnapp.com
xdn check          # verify the control plane is reachable
```

## Commands

| Command | Does | API call |
| --- | --- | --- |
| `xdn launch <name>` | Launch a service from a service config (Docker image, port, state dir, consistency). | `POST /api/v2/services/{name}` |
| `xdn service info <name>` | Show deployment info: replicas, roles, live status, geolocations. | `GET …/placement` + each replica's `…/replica/info` |
| `xdn service destroy <name>` | Permanently remove a service from the edge servers. | `DELETE /api/v2/services/{name}` |
| `xdn service move <name>` | Relocate a service's active replicas to a given set of nodes. | `PUT …/placement` |
| `xdn service leader <name> <node-id>` | Set the coordinator/leader for the service's current epoch. | `PUT …/coordinator` |
| `xdn status` | Show the status of deployed services. | `GET …/placement` |
| `xdn check` | Check connectivity / cloud-provider credentials. | — |

## Example

```bash
export XDN_CONTROL_PLANE=cp.xdnapp.com

# launch, inspect, then tear down
xdn launch my-service          # reads the service config (see Quick Start)
xdn service info my-service
xdn service destroy my-service
```

`xdn service info` queries each replica directly for its live role and container
status, so a replica that is unreachable is shown as such rather than omitted.
