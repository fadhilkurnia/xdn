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
func setupXDNPlugin(c *caddy.Controller) error {
	fmt.Printf(">> Setting up xdn plugin ...\n")
	c.Next() // 'xdn'
	c.NextArg()
	myControlPlane := c.Val() // the control plane host
	fmt.Printf(">> control plane: %s\n", myControlPlane)
	c.NextArg()
	myIPAddress := c.Val() // the nameserver ip address
	fmt.Printf(">> IP address: %s\n", myIPAddress)
	if c.NextArg() {
		return plugin.Error("xdn", c.ArgErr())
	}
	if myControlPlane == "" || myIPAddress == "" {
		return plugin.Error("xdn",
			errors.New("Need to specify the control plane and "+
				"the nameserver IP address. "+
				"Example usage: 'xdn rc.xdnapp.com 198.22.255.20'"))
	}

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		return XDN{
			xdnControlPlaneHost:      myControlPlane,
			xdnControlPlaneIPAddress: myIPAddress,
		}
	})

	return nil
}
