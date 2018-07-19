package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"

	"github.com/Azure/azure-amqp-common-go/auth"
	"github.com/Azure/azure-amqp-common-go/sas"
	"github.com/Azure/azure-amqp-common-go/uuid"
	"github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/go-autorest/autorest/to"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	maxSendTestCmd.Flags().IntVar(&msParams.messageSize, "message-size", 1024*1000, "size of messages")
	maxSendTestCmd.Flags().IntVar(&msParams.numberOfSenders, "num-senders", 10, "number of senders")
	rootCmd.AddCommand(maxSendTestCmd)
}

type (
	repeatSender struct {
		tokenProvider auth.TokenProvider
		namespace     string
		hubName       string
		messageSize   int
	}

	maxSendParams struct {
		messageSize     int
		numberOfSenders int
	}
)

func newRepeatSender(messageSize int, namespace, hubName string, provider auth.TokenProvider) *repeatSender {
	return &repeatSender{
		namespace:     namespace,
		hubName:       hubName,
		tokenProvider: provider,
		messageSize:   messageSize,
	}
}

func (s *repeatSender) Run(ctx context.Context, sentChan chan string, errChan chan error) {
	hub, err := eventhub.NewHub(s.namespace, s.hubName, s.tokenProvider, eventhub.HubWithEnvironment(environment()))
	if err != nil {
		errChan <- err
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			id, err := uuid.NewV4()
			if err != nil {
				errChan <- err
				return
			}

			data := make([]byte, messageSize)
			_, _ = rand.Read(data)
			event := eventhub.NewEvent(data)
			event.ID = id.String()
			err = hub.Send(ctx, event)

			if err != nil {
				errChan <- err
				return
			}
			sentChan <- id.String()
		}
	}
}

var (
	msParams maxSendParams

	maxSendTestCmd = &cobra.Command{
		Use:   "max-send-test",
		Short: "Send messages in parallel of a given size",
		Args: func(cmd *cobra.Command, args []string) error {
			if debug {
				log.SetLevel(log.DebugLevel)
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

			errChan := make(chan error, 1)
			defer close(errChan)
			sentChan := make(chan string, 10)
			defer close(sentChan)

			for i := 0; i < msParams.numberOfSenders; i++ {
				sender := newRepeatSender(msParams.messageSize, namespace, hubName, provider)
				go sender.Run(runCtx, sentChan, errChan)
			}

			// Wait for a signal to quit:
			signalChan := make(chan os.Signal, 1)
			signal.Notify(signalChan, os.Interrupt, os.Kill)

			count := 0
			for {
				select {
				case <-signalChan:
					log.Println("closing via OS signal...")
					runCancel()
					return
				case err := <-errChan:
					log.Error(err)
					runCancel()
					return
				case _ = <-sentChan:
					count++
					if count % 10000 == 0 {
						log.Printf("Sent: %d", count)
					}
				}
			}

		},
	}
)

func ensureProvisioned(ctx context.Context) (*eventhub.HubEntity, error) {
	hm, err := eventhub.NewHubManagerFromConnectionString(connStr)
	if err != nil {
		return nil, err
	}

	hubs, err := hm.List(ctx)
	if err != nil {
		return nil, err
	}

	for _, hub := range hubs {
		fmt.Printf("%+v", hub)
		if hub.Name == hubName {
			return hub, nil
		}
	}

	return hm.Put(ctx, hubName, eventhub.HubDescription{
		PartitionCount: to.Int32Ptr(128),
	})
}
