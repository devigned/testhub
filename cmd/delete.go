package cmd

import (
	"context"

	"github.com/Azure/azure-event-hubs-go/v2"
	"github.com/cloudflare/cfssl/log"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(deleteCmd)
}

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete an Event Hub",
	Args: func(cmd *cobra.Command, args []string) error {
		return checkAuthFlags()
	},
	Run: func(cmd *cobra.Command, args []string) {
		hm, err := eventhub.NewHubManagerFromConnectionString(connStr)
		if err != nil {
			log.Error(err)
			return
		}
		err = hm.Delete(context.Background(), hubName)
		if err != nil {
			log.Error(err)
		}
		log.Infof("deleted %q", hubName)
	},
}
