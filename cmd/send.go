package cmd

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/Azure/azure-amqp-common-go/sas"
	"github.com/Azure/azure-event-hubs-go"
	"github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	sendCmd.Flags().IntVar(&messageCount, "msg-count", 10, "number of messages to send")
	sendCmd.Flags().IntVar(&messageSize, "msg-size", 256, "size in bytes of each message")
	rootCmd.AddCommand(sendCmd)
}

var (
	messageCount, messageSize int

	sendCmd = &cobra.Command{
		Use:   "send",
		Short: "Send messages to an Event Hub",
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
			hub, err := eventhub.NewHub(namespace, hubName, provider, eventhub.HubWithEnvironment(environment()))
			if err != nil {
				log.Error(err)
				return
			}

			log.Println(fmt.Sprintf("attempting to send %d messages", messageCount))
			sentMsgs := 0
			for i := 0; i < messageCount; i++ {
				data := make([]byte, messageSize)
				_, err := rand.Read(data)
				if err != nil {
					log.Errorln("unable to generate random bits for message")
					continue
				}
				event := eventhub.NewEvent(data)

				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
				span, ctx := opentracing.StartSpanFromContext(ctx, "cmd_send")
				err = hub.Send(ctx, event)
				if err != nil {
					log.Errorln(fmt.Sprintf("failed sending idx: %d", i), err)
				} else {
					sentMsgs++
				}
				cancel()
				span.Finish()
			}

			log.Printf("sent %d messages\n", sentMsgs)
		},
	}
)
