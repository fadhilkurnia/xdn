package cmd

import "github.com/spf13/cobra"

var StatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show status of the deployed web services",
}
