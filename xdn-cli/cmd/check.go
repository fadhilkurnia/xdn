package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var CheckCmd = &cobra.Command{
	Use:   "check",
	Short: "Check credentials of cloud providers or onprem",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		check()
	},
}

func check() {
	fmt.Println("Checking credentials to enable XDN deployment.")

	isOnPremEnabled, _ := checkOnPrem()
	fmt.Printf("  On-premises : %s\n",
		isEnabledBoolToString(isOnPremEnabled))

	isAWSEnabled, _ := checkAWS()
	fmt.Printf("  AWS         : %s\n",
		isEnabledBoolToString(isAWSEnabled))

	isGCPEnabled, _ := checkGCP()
	fmt.Printf("  GCP         : %s\n",
		isEnabledBoolToString(isGCPEnabled))

	isAzureEnabled, _ := checkAzure()
	fmt.Printf("  Azure       : %s\n",
		isEnabledBoolToString(isAzureEnabled))

	fmt.Printf("  Oracle      : coming soon\n")
	fmt.Printf("  Alibaba     : coming soon\n")
	fmt.Printf("  Hetzner     : coming soon\n")

	if !isOnPremEnabled &&
		!isAWSEnabled &&
		!isGCPEnabled &&
		!isAzureEnabled {
		fmt.Printf("XDN requires at least one cloud or onprem enabled.\n")
	}

	fmt.Printf("Please see our documentation to configure the cloud credentials.\n")
}

func isEnabledBoolToString(isEnabled bool) string {
	if isEnabled {
		return "enabled"
	}
	return "disabled"
}

func checkOnPrem() (bool, error) {
	return false, nil
}

func checkAWS() (bool, error) {
	return false, nil
}

func checkGCP() (bool, error) {
	return false, nil
}

func checkAzure() (bool, error) {
	return false, nil
}
