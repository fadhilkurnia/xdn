package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"net/http"
	"net/url"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"sigs.k8s.io/yaml"
)

const XDN_BASE_HOST = "xdnapp.com"

type CommonProperties struct {
	serviceName       string
	imageName         string
	httpPort          int
	consistencyModel  string
	isDeterministic   bool
	stateDir          string
	rawJsonProperties string
}

var LaunchCmd = &cobra.Command{
	Use:   "launch <service-name>",
	Short: "Launch a web service on edge servers",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		serviceName := args[0]

		fileName, err := cmd.Flags().GetString("file")
		if err != nil {
			fmt.Printf("failed to read file of service properties")
			return
		}

		prop := CommonProperties{}

		// case-1: service property file is provided, ignore other flags
		if fileName != "" {
			prop, err = parseDeclaredPropertiesFromFile(fileName)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if serviceName != prop.serviceName {
				fmt.Println("unmatch service name specified in the file")
				return
			}
		}

		// case-2: service property file is not provided
		if fileName == "" {
			prop, err = parseDeclaredPropertiesFromFlags(serviceName, cmd.Flags())
			if err != nil {
				fmt.Println(err.Error())
				return
			}
		}

		runLaunchCommand(prop)
	},
}

func init() {
	LaunchCmd.PersistentFlags().StringP("image", "i", "", "docker image name")
	LaunchCmd.PersistentFlags().IntP("port", "p", 80, "service's port to serve HTTP requests")
	LaunchCmd.PersistentFlags().StringP("consistency", "c", "linearizability", "consistency model for the replicated service")
	LaunchCmd.PersistentFlags().StringP("state", "s", "/", "absolute path of directory in which the service store the state durably")
	LaunchCmd.PersistentFlags().BoolP("deterministic", "d", false, "indicate whether the service is deterministic (default: false)")

	// Note: if file is specified, properties specified by flags will be ignored
	LaunchCmd.Flags().StringP("file", "f", "", "indicate file location containing the service's properties")
}

func parseDeclaredPropertiesFromFlags(serviceName string, flags *pflag.FlagSet) (CommonProperties, error) {
	var err error
	prop := CommonProperties{}

	// TODO: limit serviceName length, check valid chars
	prop.serviceName = serviceName
	if serviceName == "" {
		errMsg := "service name is required"
		fmt.Print(errMsg + ".\n")
		return prop, errors.New(errMsg)
	}

	// TODO: check valid docker image name from illegal chars
	prop.imageName, err = flags.GetString("image")
	if err != nil || prop.imageName == "" {
		fmt.Printf("docker image name is required, set with --image flag.\n")
		return prop, err
	}

	// parse the web service exposed port
	prop.httpPort, err = flags.GetInt("port")
	if err != nil || prop.httpPort == 0 {
		fmt.Printf("the service's port is required, set with --port flag.\n")
		return prop, err
	}

	// parse the requested consistency model
	prop.consistencyModel = "linearizability"
	if c, err := flags.GetString("consistency"); err != nil || c != "" {
		prop.consistencyModel = c
		// TODO: validate consistency models
	}

	// parse whether the web service is deterministic or not
	prop.isDeterministic = false
	d, err := flags.GetBool("deterministic")
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
	}
	prop.isDeterministic = d

	// TODO: validate state directory, should not have any whitespace
	prop.stateDir, err = flags.GetString("state")
	if err != nil || prop.stateDir == "" {
		fmt.Printf("the service's state directory is required, set with --state flag.\n")
		return prop, err
	}

	prop.rawJsonProperties = fmt.Sprintf(`
	{
		"name":"%s",
		"image":"%s",
		"port":%d,
		"state":"%s",
		"consistency":"%s",
		"deterministic":%t
	}`, prop.serviceName,
		prop.imageName,
		prop.httpPort,
		prop.stateDir,
		prop.consistencyModel,
		prop.isDeterministic)
	trimmedJson := strings.ReplaceAll(prop.rawJsonProperties, " ", "")
	trimmedJson = strings.ReplaceAll(trimmedJson, "\t", "")
	trimmedJson = strings.ReplaceAll(trimmedJson, "\n", "")
	prop.rawJsonProperties = trimmedJson

	return prop, nil
}

func parseDeclaredPropertiesFromFile(fileName string) (CommonProperties, error) {
	prop := CommonProperties{}

	body, err := os.ReadFile(fileName)
	if err != nil {
		return prop, fmt.Errorf("unable to open file %s", fileName)
	}

	jsonBody, err := yaml.YAMLToJSON(body)
	if err != nil {
		return prop,
			fmt.Errorf("unable to parse service properties %s", err.Error())
	}

	prop.rawJsonProperties = string(jsonBody)

	// TODO: parse some of the parsed properties
	propMap := make(map[string]interface{})
	err = json.Unmarshal(jsonBody, &propMap)
	if err != nil {
		return prop, err
	}

	if propMap["components"] == nil {
		prop.serviceName = propMap["name"].(string)
		prop.imageName = propMap["image"].(string)
		prop.httpPort = int(propMap["port"].(float64))
		prop.consistencyModel = propMap["consistency"].(string)
		prop.stateDir = propMap["state"].(string)
		if propMap["deterministic"] != nil {
			prop.isDeterministic = propMap["deterministic"].(bool)
		}
	} else {
		componentMap := propMap["components"].([]interface{})
		images := ""
		componentCounter := 0
		for _, components := range componentMap {
			componentCounter++
			isLast := componentCounter == len(componentMap)

			for componentName, componentPropIf := range components.(map[string]interface{}) {
				componentProp := componentPropIf.(map[string]interface{})
				componentImage := componentProp["image"].(string)
				images += fmt.Sprintf("%s:%s", componentName, componentImage)
				if !isLast {
					images += "\n                 "
				}
				if componentProp["port"] != nil {
					prop.httpPort = int(componentProp["port"].(float64))
				}
			}
		}
		prop.serviceName = propMap["name"].(string)
		prop.imageName = images
		prop.consistencyModel = propMap["consistency"].(string)
		prop.stateDir = propMap["state"].(string)
		if propMap["deterministic"] != nil {
			prop.isDeterministic = propMap["deterministic"].(bool)
		}
	}

	return prop, nil
}

func runLaunchCommand(prop CommonProperties) {
	colorPrint := color.New(color.FgYellow).Add(color.Bold).Add(color.Underline)
	errColorPrint := color.New(color.FgRed).Add(color.Bold).Add(color.Underline)

	fmt.Printf("Launching ")
	colorPrint.Printf("%s", prop.serviceName)
	fmt.Printf(" service with the following configuration:\n")
	fmt.Printf(" docker image  : %s\n", prop.imageName)
	fmt.Printf(" http port     : %d\n", prop.httpPort)
	fmt.Printf(" consistency   : %s\n", prop.consistencyModel)
	fmt.Printf(" deterministic : %t\n", prop.isDeterministic)
	fmt.Printf(" state dir     : %s\n\n", prop.stateDir)

	// checking connection to the control plane
	timeout := 1 * time.Second
	controlPlane := os.Getenv("XDN_CONTROL_PLANE")
	if controlPlane == "" {
		controlPlane = DEFAULT_CONTROL_PLANE
	}
	controlPlaneHost := fmt.Sprintf("%s:%d", controlPlane, CONTROL_PLANE_PORT)
	_, err := net.DialTimeout("tcp", controlPlaneHost, timeout)
	if err != nil {
		fmt.Printf(" ")
		errColorPrint.Printf("ERROR")
		fmt.Printf(":")
		fmt.Printf(" Cannot reach XDN control plane at `%s`.\n\n", controlPlane)
		fmt.Printf(" Is the Control Plane running there?\n\n")
		fmt.Printf(" Alternatively use another XDN Control Plane by specifying\n")
		fmt.Printf(" the environment variable, for example:\n")
		fmt.Printf("   export XDN_CONTROL_PLANE=cp.xdnapp.com\n\n")
		return
	}

	// contact the control plane to actually deploy the service
	encodedInitialState := fmt.Sprintf("xdn:init:%s", prop.rawJsonProperties)
	gigapaxosEndpoint := fmt.Sprintf(
		"http://%s/?type=CREATE&name=%s&initial_state=%s",
		controlPlaneHost, prop.serviceName,
		url.PathEscape(encodedInitialState))
	resp, err := http.Get(gigapaxosEndpoint)
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			fmt.Printf(" ")
			errColorPrint.Printf("ERROR")
			fmt.Printf(": Cannot reach XDN control plane at `%s`.\n\n",
				controlPlane)
			fmt.Printf(" Is the Control Plane running there?\n\n")
			return
		}
		fmt.Printf(" ")
		errColorPrint.Printf("ERROR")
		fmt.Printf(": Failed to launch the service: \n%s\n", err.Error())
		return
	}
	if resp.StatusCode != 200 {
		fmt.Printf(" ")
		errColorPrint.Printf("ERROR")
		fmt.Printf(": Failed to launch the service, received non success code.\n")
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf(" ")
		errColorPrint.Printf("ERROR")
		fmt.Printf(": Failed to launch the service: \n%s\n", err.Error())
		return
	}

	bodyStr := string(body)
	if strings.Contains(bodyStr, "\"FAILED\":true") {
		fmt.Printf(" ")
		errColorPrint.Printf("ERROR")
		fmt.Printf(" ")
		fmt.Printf("Failed to launch the service: \n")
		var jsonMap map[string]interface{}
		json.Unmarshal([]byte(bodyStr), &jsonMap)
		errMsgIf := jsonMap["RESPONSE_MESSAGE"]
		errMsg := errMsgIf.(string)
		fmt.Printf(" %s\n", errMsg)
		return
	}

	dummyServiceURL := colorPrint.Sprintf(
		"http://%s.%s/", prop.serviceName, XDN_BASE_HOST)
	fmt.Println("The service is successfully launched 🎉🚀 ")
	if controlPlane != DEFAULT_CONTROL_PLANE {
		fmt.Printf("Access your service at the following permanent URL:\n")
		fmt.Printf("  > %s     \n\n\n", dummyServiceURL)
	} else {
		fmt.Printf("\n")
	}
	fmt.Printf("Retrieve the service's replica locations with this command:\n")
	fmt.Printf("  xdn service info %s\n", prop.serviceName)
	fmt.Printf("Destroy the replicated service with this command:\n")
	fmt.Printf("  xdn service destroy %s\n", prop.serviceName)
	fmt.Println()

	// fmt.Printf("which will redirect you to one of the following service replicas:\n")
	// fmt.Printf("  - http://%s.AR0.xdn.io/\n", serviceName)
	// fmt.Printf("  - http://%s.AR1.xdn.io/\n", serviceName)
	// fmt.Printf("  - http://%s.AR2.xdn.io/\n", serviceName)
}
