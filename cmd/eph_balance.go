package cmd

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"time"

	"github.com/Azure/azure-event-hubs-go/v2/eph"
	"github.com/Azure/azure-event-hubs-go/v2/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
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
			runCtx, runCancel := context.WithCancel(context.Background())
			defer runCancel()

			_, err := ensureProvisioned(runCtx)
			if err != nil {
				log.Error(err)
				return
			}

			cred, err := azblob.NewSharedKeyCredential(ephBalanceParams.storageAccountName, ephBalanceParams.storageAccountKey)
			if err != nil {
				log.Error(err)
				return
			}

			checkpointLeaser, err := storage.NewStorageLeaserCheckpointer(cred, ephBalanceParams.storageAccountName, ephBalanceParams.storageContainer, azure.PublicCloud)
			if err != nil {
				log.Error(err)
				return
			}

			host, err := eph.NewFromConnectionString(runCtx, connStr, checkpointLeaser, checkpointLeaser)
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
				lastTracked := make([]int, 0)
				for {
					time.Sleep(30 * time.Second)
					select {
					case <-runCtx.Done():
						return
					default:
						partitionIDs := host.PartitionIDsBeingProcessed()
						ints := make([]int, len(partitionIDs))
						for idx, id := range partitionIDs {
							i, err := strconv.Atoi(id)
							if err != nil {
								log.Error(err)
								return
							}
							ints[idx] = i
						}
						sort.Ints(ints)
						log.Infof("number of partitions: %d, %+v", len(partitionIDs), ints)
						added, removed := intDiff(lastTracked, ints)
						log.Infof("added: %+v; removed: %+v", added, removed)
						lastTracked = ints
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

func intDiff(original []int, updated []int) (added []int, removed []int) {
	originalMap := make(map[int]int)
	for i := 0; i < len(original); i++ {
		originalMap[original[i]] = original[i]
	}

	updatedMap := make(map[int]int)
	for i := 0; i < len(updated); i++ {
		updatedMap[updated[i]] = updated[i]
		if _, ok := originalMap[updated[i]]; !ok {
			// if not in the original, then you were added
			added = append(added, updated[i])
		}
	}

	for i := 0; i < len(original); i++ {
		if _, ok := updatedMap[original[i]]; !ok {
			// if not in the original, then you were added
			removed = append(removed, original[i])
		}
	}
	return added, removed
}
