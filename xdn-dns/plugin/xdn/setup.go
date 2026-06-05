package xdn

import (
	"errors"
	"fmt"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
)

func init() { plugin.Register("xdn", setupXDNPlugin) }

// Parses the configuration from the Corefile.
// Example of a valid config:
//
//	xdn rc.xdnapp.com 198.22.255.20
//
// specifying the control plane (e.g., cp.xdnapp.com) and the nameserver
// IP address (the machine where CoreDNS is running).
//
// An optional block maps each ActiveReplica node id to its GLOBAL IPv6, so the
// plugin can answer per-replica names under the reserved `edge` sub-zone
// (<nodeid>.edge.<base_domain> -> that node's IPv6), e.g. for browser-clickable
// per-replica service links served by a specific replica:
//
//	xdn cp.xdnapp.com 198.22.255.20 {
//	    edge useast1a 2600:1f18:2a49:8c01::a
//	    edge useast1b 2600:1f18:2a49:8c02::a
//	}
func setupXDNPlugin(c *caddy.Controller) error {
	fmt.Printf(">> Setting up xdn plugin ...\n")
	c.Next() // 'xdn'
	c.NextArg()
	myControlPlane := c.Val() // the control plane host
	fmt.Printf(">> control plane: %s\n", myControlPlane)
	c.NextArg()
	myIPAddress := c.Val() // the nameserver ip address
	fmt.Printf(">> IP address: %s\n", myIPAddress)
	if myControlPlane == "" || myIPAddress == "" {
		return plugin.Error("xdn",
			errors.New("Need to specify the control plane and "+
				"the nameserver IP address. "+
				"Example usage: 'xdn rc.xdnapp.com 198.22.255.20'"))
	}

	// Optional block: `edge <nodeid> <ipv6>` mappings for the reserved edge sub-zone.
	edgeNodes := map[string]string{}
	for c.NextBlock() {
		switch c.Val() {
		case "edge":
			args := c.RemainingArgs() // [nodeid, ipv6]
			if len(args) != 2 {
				return plugin.Error("xdn", c.ArgErr())
			}
			edgeNodes[args[0]] = args[1]
			fmt.Printf(">> edge node: %s -> %s\n", args[0], args[1])
		default:
			return plugin.Error("xdn", c.Errf("unknown xdn property %q", c.Val()))
		}
	}

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		return XDN{
			xdnControlPlaneHost:      myControlPlane,
			xdnControlPlaneIPAddress: myIPAddress,
			edgeNodes:                edgeNodes,
		}
	})

	return nil
}
