package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var ServiceRootCmd = &cobra.Command{
	Use:   "service <action>",
	Short: "Does an action toward a launched replicated web services",
}

var ServiceInfoCmd = &cobra.Command{
	Use:   "info <service-name>",
	Short: "Get deployment info about the replicated web service",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		infoColorPrint := color.New(color.FgYellow).Add(color.Bold)
		successColorPrint := color.New(color.FgGreen).Add(color.Bold).Add(color.Underline)
		errorColorPrint := color.New(color.FgRed).Add(color.Bold).Add(color.Underline)
		dummyColorPrint := color.New(color.FgRed)

		err := ValidateControlPlaneConn()
		if err != nil {
			errorColorPrint.Printf(" ERROR ")
			fmt.Errorf("Failed to reach the control plane: %s.\n", err.Error())
			return
		}

		serviceName := args[0]
		fmt.Printf("Getting info for '")
		infoColorPrint.Printf("%s", serviceName)
		fmt.Printf("' ...\n")

		controlPlaneHost := GetControlPlaneHostPort()
		infoEndpoint := fmt.Sprintf("http://%s/?type=REQ_ACTIVES&name=%s",
			controlPlaneHost, serviceName)
		resp, err := http.Get(infoEndpoint)
		if err != nil {
			fmt.Printf(" ")
			errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Errorf("Failed to get the deployment info: %s", err.Error())
			return
		}
		if resp.StatusCode != 200 {
			fmt.Printf(" ")
			errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Failed to get deployment info, received non success code.\n")
			return
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf(" ")
			errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Failed to get deployment info: \n%s\n", err.Error())
			return
		}

		bodyStr := string(body)
		if strings.Contains(bodyStr, "\"FAILED\":true") {
			fmt.Printf(" ")
			errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Failed to get deployment info:\n")
			var jsonMap map[string]interface{}
			json.Unmarshal([]byte(bodyStr), &jsonMap)
			errMsgIf := jsonMap["RESPONSE_MESSAGE"]
			errMsg := errMsgIf.(string)
			fmt.Printf(" %s\n", errMsg)
			return
		}

		successColorPrint.Printf("SUCCESS")
		fmt.Printf(", current deployment information:\n\n")

		// try to parse the success response
		var jsonMap map[string]interface{}
		err = json.Unmarshal([]byte(bodyStr), &jsonMap)
		if err != nil {
			fmt.Errorf("Failed to parse the success response, %s\n",
				err.Error())
			return
		}

		// parse the service name
		serviceNameIf := jsonMap["NAME"]
		serviceNameStr := "?"
		if serviceNameIf != nil {
			if sn, ok := serviceNameIf.(string); ok {
				serviceNameStr = sn
			}
		}

		// parse the placement epoch number
		epochNumberIf := jsonMap["EPOCH"]
		epochNumberStr := "?"
		if epochNumberIf != nil {
			if num, ok := epochNumberIf.(float64); ok {
				epochNumber := int64(num)
				epochNumberStr = fmt.Sprintf("%d", epochNumber)
			}
		}

		// parse the replica ip addresses
		replicaAddressIf := jsonMap["ACTIVE_REPLICAS"]
		numReplicas := 0
		replicaAddressList := make([]string, numReplicas)
		if replicaAddressIf != nil {
			if ls, ok := replicaAddressIf.([]interface{}); ok {
				numReplicas = len(ls)
				for _, rawAddress := range ls {
					rawAddressStr := ""
					if addressStr, ok := rawAddress.(string); ok {
						rawAddressStr = addressStr
					} else {
						continue
					}
					rawAddressComponents := strings.Split(rawAddressStr, ":")
					if len(rawAddressComponents) != 2 {
						panic("unexpected address format")
					}
					ipAddress := rawAddressComponents[0][1:]
					replicaAddressList = append(replicaAddressList, ipAddress)
				}
			}
		}

		// TODO: query one of the replica to get more about the service details,
		//  for now we are populating dummy data here.
		dockerImageName := dummyColorPrint.Sprint("fadhilkurnia/xdn-bookcatalog")
		httpPort := dummyColorPrint.Sprint("80")
		consistencyModel := dummyColorPrint.Sprint("linearizability")
		isDeterministic := dummyColorPrint.Sprint("true")
		stateDir := dummyColorPrint.Sprint("/app/data/")
		coordinationProtocolName := dummyColorPrint.Sprint("MultiPaxos")

		fmt.Printf(" service name  : %s \n", serviceNameStr)
		fmt.Printf(" service url   : http://%s.xdnapp.com/ \n", serviceNameStr)
		fmt.Printf(" docker image  : %s \n", dockerImageName)
		fmt.Printf(" http port     : %s (internal) \n", httpPort)
		fmt.Printf(" consistency   : %s \n", consistencyModel)
		fmt.Printf(" deterministic : %s \n", isDeterministic)
		fmt.Printf(" state dir.    : %s \n", stateDir)
		fmt.Printf(" num. replica  : %d \n", numReplicas)
		fmt.Printf(" protocol      : %s \n", coordinationProtocolName)
		fmt.Printf("\n")
		fmt.Printf(" Current replicas placement, with epoch=%s:\n",
			epochNumberStr)
		fmt.Printf("  | %-10s | %-24s | %-16s | %-16s | %-16s |\n",
			fmt.Sprint("MACHINE ID"),
			fmt.Sprint("PUBLIC IP"),
			fmt.Sprint("ROLE"),
			fmt.Sprint("CREATED"),
			fmt.Sprint("STATUS"))
		for idx, address := range replicaAddressList {
			roleStr := dummyColorPrint.Sprint("primary")
			createdStr := dummyColorPrint.Sprint("2 minutes ago")
			statusStr := dummyColorPrint.Sprint("Up 2 minutes")
			if idx != 0 {
				roleStr = dummyColorPrint.Sprint("backup ")
			}
			fmt.Printf("  | %-10s | %-24s | %-16s | %-16s | %-16s |\n",
				fmt.Sprintf("AR%d", idx),
				fmt.Sprintf("%s", address),
				fmt.Sprintf("%s         ", roleStr),
				fmt.Sprintf("%s   ", createdStr),
				fmt.Sprintf("%s    ", statusStr))
		}

		fmt.Printf("\n\n")
		fmt.Printf(" Declared service's operation properties:\n")
		fmt.Printf("  - %s\n", dummyColorPrint.Sprint("GET /            read-only"))
		fmt.Printf("  - %s\n", dummyColorPrint.Sprint("GET /api/books   read-only"))
		fmt.Printf("\n")

		// | AR1        | 172.0.0.1  | primary | 2 minutes ago | Up 2 minutes |
		// | AR2        | 172.0.0.2  | backup  |               | Up 2 minutes |
		// | AR3        | 172.0.0.2  | backup  |               | Up 2 minutes |
		//
		// Declared service's operation properties:
		// - GET / read-only

		fmt.Printf("Note that currently %s are still dummy information, "+
			"we are working to implement them\nafter other prioritized research agendas are done.\n",
			dummyColorPrint.Sprint("all texts in red"))

	},
}

var ServiceDestroyCmd = &cobra.Command{
	Use:   "destroy <service-name>",
	Short: "Permanently remove a replicated web service from edge servers",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		infoColorPrint := color.New(color.FgYellow).Add(color.Bold)
		successColorPrint := color.New(color.FgGreen).Add(color.Bold).Add(color.Underline)
		errorColorPrint := color.New(color.FgRed).Add(color.Bold).Add(color.Underline)

		err := ValidateControlPlaneConn()
		if err != nil {
			fmt.Errorf("Failed to reach the control plane: %s.\n", err.Error())
			return
		}

		serviceName := args[0]
		isValidInput := false
		isRemoveConfirmed := false
		for !isValidInput {
			fmt.Printf(
				"Are you sure you want to remove `%s` service? [yes/no]\n > ",
				serviceName)
			input := bufio.NewScanner(os.Stdin)
			input.Scan()
			isSureText := input.Text()
			if isSureText == "yes" || isSureText == "no" {
				isValidInput = true
				if isSureText == "yes" {
					isRemoveConfirmed = true
				}
				break
			}
			fmt.Printf(
				"  '%s' is not a valid answer, "+
					"please answer with 'yes' or 'no'.\n",
				isSureText)
		}

		if !isRemoveConfirmed {
			fmt.Printf("  '%s' is not removed.\n", serviceName)
			return
		}

		controlPlaneHost := GetControlPlaneHostPort()
		infoEndpoint := fmt.Sprintf("http://%s/?type=DELETE&name=%s",
			controlPlaneHost, serviceName)
		fmt.Printf("Removing service '")
		infoColorPrint.Printf("%s", serviceName)
		fmt.Printf("' ...\n")
		resp, err := http.Get(infoEndpoint)
		if err != nil {
			fmt.Errorf("Failed to destroy the service, error: %s", err.Error())
			return
		}

		if resp.StatusCode != 200 {
			fmt.Printf(" ")
			errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Failed to remove the service, received non success code.\n")
			return
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf(" ")
			errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Failed to remove the service: \n%s\n", err.Error())
			return
		}

		bodyStr := string(body)
		if strings.Contains(bodyStr, "\"FAILED\":true") {
			fmt.Printf(" ")
			errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Failed to remove the service: \n")
			var jsonMap map[string]interface{}
			json.Unmarshal([]byte(bodyStr), &jsonMap)
			errMsgIf := jsonMap["RESPONSE_MESSAGE"]
			errMsg := errMsgIf.(string)
			fmt.Printf(" %s\n", errMsg)
			return
		}

		successColorPrint.Printf("SUCCESS")
		fmt.Printf(": service ")
		infoColorPrint.Printf("%s", serviceName)
		fmt.Printf(" is removed successfully.\n")
	},
}

func init() {
	ServiceRootCmd.AddCommand(ServiceDestroyCmd)
	ServiceRootCmd.AddCommand(ServiceInfoCmd)
}
