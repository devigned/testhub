package cmd

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"time"

	"github.com/Azure/azure-amqp-common-go/sas"
	"github.com/Azure/azure-event-hubs-go/eph"
	"github.com/Azure/azure-event-hubs-go/storage"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	ephBalanceTestCmd.Flags().StringVar(&ephBalanceParams.storageAccountName, "storage-account-name", "", "Azure Storage account name to use for blob based leader election")
	ephBalanceTestCmd.Flags().StringVar(&ephBalanceParams.storageAccountKey, "storage-account-key", "", "Azure Storage account key to use for blob based leader election")
	ephBalanceTestCmd.Flags().StringVar(&ephBalanceParams.storageContainer, "storage-container", "ephbalancetest", "Azure Storage container name for storing blobs")
	rootCmd.AddCommand(ephBalanceTestCmd)
}

type (
	ephBalanceParamSet struct {
		storageAccountName string
		storageAccountKey  string
		storageContainer   string
	}
)

var (
	ephBalanceParams ephBalanceParamSet

	ephBalanceTestCmd = &cobra.Command{
		Use:   "eph-balance-test",
		Short: "Run Event Host Processor and output partition balance details",
		Args: func(cmd *cobra.Command, args []string) error {
			if debug {
				log.SetLevel(log.DebugLevel)
			}

			if ephBalanceParams.storageAccountName == "" {
				return errors.New("--storage-account-name must be specified")
			}

			if ephBalanceParams.storageAccountKey == "" {
				return errors.New("--storage-account-key must be specified")
			}

			return checkAuthFlags()
		},
		Run: func(cmd *cobra.Command, args []string) {
			provider, err := sas.NewTokenProvider(sas.TokenProviderWithKey(sasKeyName, sasKey))
			if err != nil {
				log.Error(err)
				return
			}

			runCtx, runCancel := context.WithCancel(context.Background())
			defer runCancel()

			_, err = ensureProvisioned(runCtx)
			if err != nil {
				log.Error(err)
				return
			}

			cred := azblob.NewSharedKeyCredential(ephBalanceParams.storageAccountName, ephBalanceParams.storageAccountKey)
			checkpointLeaser, err := storage.NewStorageLeaserCheckpointer(cred, ephBalanceParams.storageAccountName, ephBalanceParams.storageContainer, azure.PublicCloud)
			if err != nil {
				log.Error(err)
				return
			}

			host, err := eph.New(runCtx, namespace, hubName, provider, checkpointLeaser, checkpointLeaser)
			if err != nil {
				log.Error(err)
				return
			}

			err = host.StartNonBlocking(runCtx)
			if err != nil {
				log.Error(err)
				return
			}

			go func() {
				// report partition processing for the host
				for {
					time.Sleep(30 * time.Second)
					select {
					case <-runCtx.Done():
						return
					default:
						partitionIDs := host.PartitionIDsBeingProcessed()
						log.Infof("number of partitions: %d, 	%+v", len(partitionIDs), partitionIDs)
					}
				}
			}()

			// Wait for a signal to quit:
			signalChan := make(chan os.Signal, 1)
			signal.Notify(signalChan, os.Interrupt, os.Kill)

			select {
			case <-signalChan:
				log.Println("closing via OS signal...")
				runCancel()
				return
			}

		},
	}
)
