package cmd

import (
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

// By default we use localhost:3300 as the control plane host when environment
// variable of `XDN_CONTROL_PLANE` is not set. Otherwise, we use the value of
// `XDN_CONTROL_PLANE` at port 443 (https).
const CONTROL_PLANE_PORT = 3300
const DEFAULT_CONTROL_PLANE = "localhost"

var xdnLongHelp = `
                    ____  ______________   __ 
                    __  |/ /__  __ \__  | / /   
                    __    /__  / / /_   |/ /    
                    _    | _  /_/ /_  /|  /__   
                    /_/|_| /_____/ /_/ |_/_(_)  
                    
                    Edge Service Delivery Network

XDN is an upgrade for stateless CDN. Deploy your stateful web services 
anywhere on earth easily with XDN via declarative replication.
XDN is a research protytpe from the University of Massachusetts.

Documentation is available at https://xdn.cs.umass.edu/.`

var rootCmd = &cobra.Command{
	Use:   "xdn",
	Short: "XDN is a tool to deploy stateful web service on edges",
	Long:  xdnLongHelp,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			err := cmd.Help()
			if err != nil {
				fmt.Println("error: ", err)
				os.Exit(1)
			}
			return
		}
		fmt.Printf("xdn: '%s' is not a valid command.\n"+
			"See 'xdn --help'\n",
			args[0])
	},
}

func init() {
	cobra.OnInitialize()
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	rootCmd.AddCommand(LaunchCmd)
	rootCmd.AddCommand(ServiceRootCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			fmt.Println("error: ", printErr)
			os.Exit(1)
		}
		os.Exit(1)
	}
}

// Validates connection to XDN control plane, if it is not accessible this
// method prints out the error and return a non-empty error, otherwise nil error
// is returned. We use the control plane specified in the environment variable
// of 'XDN_CONTROL_PLANE', if not set we use 'localhost:3300' by default.
func ValidateControlPlaneConn() error {
	errColorPrint := color.New(color.FgRed).Add(color.Bold).Add(color.Underline)

	// parse the control plane from env variable
	controlPlane := os.Getenv("XDN_CONTROL_PLANE")
	if controlPlane == "" {
		controlPlane = DEFAULT_CONTROL_PLANE
	}

	// checking connection to the control plane
	controlPlaneHost := fmt.Sprintf("%s:%d", controlPlane, CONTROL_PLANE_PORT)
	timeout := 1 * time.Second
	_, err := net.DialTimeout("tcp", controlPlaneHost, timeout)
	if err != nil {
		// printout the error
		fmt.Printf(" ")
		errColorPrint.Printf("ERROR")
		fmt.Printf(": ")
		fmt.Printf("Cannot reach XDN control plane at `%s`.\n\n ", controlPlane)
		fmt.Printf("Is the Control Plane running there?\n\n ")
		fmt.Printf("Alternatively use another XDN Control Plane by ")
		fmt.Printf("specifying\n the environment variable, for example:\n")
		fmt.Printf("   export XDN_CONTROL_PLANE=cp.xdnapp.com\n\n")

		// prepare the returned error
		errMsg := fmt.Sprintf(
			"Cannot reach XDN control plane at `%s`", controlPlane)
		return errors.New(errMsg)
	}

	return nil
}

// Returns a string containing the host and port of the control plane,
// separated with ':' character.
func GetControlPlaneHostPort() string {
	// parse the control plane from env variable
	controlPlane := os.Getenv("XDN_CONTROL_PLANE")
	if controlPlane == "" {
		controlPlane = DEFAULT_CONTROL_PLANE
	}
	controlPlaneHost := fmt.Sprintf("%s:%d", controlPlane, CONTROL_PLANE_PORT)
	return controlPlaneHost
}
