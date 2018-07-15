package cmd

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"time"

	"github.com/Azure/azure-amqp-common-go/auth"
	"github.com/Azure/azure-amqp-common-go/sas"
	"github.com/Azure/azure-amqp-common-go/uuid"
	"github.com/Azure/azure-event-hubs-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(sparseCmd)
}

type (
	uniformDistributedPeriodicSender struct {
		tokenProvider auth.TokenProvider
		namespace     string
		hubName       string
		maxSeconds    int
	}
)

func newUniformDistributedPeriodicSender(maxSeconds int, namespace, hubName string, provider auth.TokenProvider) *uniformDistributedPeriodicSender {
	u := new(uniformDistributedPeriodicSender)
	u.namespace = namespace
	u.hubName = hubName
	u.tokenProvider = provider
	u.maxSeconds = maxSeconds
	return u
}

func (u *uniformDistributedPeriodicSender) Run(ctx context.Context, errChan chan error) {
	defer close(errChan)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			delay := time.Duration(rand.Intn(u.maxSeconds)) * time.Second
			time.Sleep(delay)

			hub, err := eventhub.NewHub(u.namespace, u.hubName, u.tokenProvider, eventhub.HubWithEnvironment(environment()))
			if err != nil {
				errChan <- err
				return
			}

			id, err := uuid.NewV4()
			if err != nil {
				errChan <- err
				return
			}
			event := eventhub.NewEvent([]byte(id.String()))
			event.ID = id.String()
			err = hub.Send(ctx, event)
			if err != nil {
				errChan <- err
				return
			}
			log.Printf("Sent: %q", event.ID)
			err = hub.Close(ctx)
			if err != nil {
				errChan <- err
				return
			}
		}
	}
}

var (
	//params sparseParams

	sparseCmd = &cobra.Command{
		Use:   "sparse-test",
		Short: "Send and Receive sparse messages over a long period of time",
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

			hub, err := eventhub.NewHub(namespace, hubName, provider, eventhub.HubWithEnvironment(environment()))
			if err != nil {
				log.Error(err)
				return
			}
			defer hub.Close(runCtx)

			errChan := make(chan error, 1)
			go listenToAll(runCtx, hub, func(ctx context.Context, evt *eventhub.Event) error {
				log.Printf("Received: %q", evt.ID)
				return nil
			}, errChan)

			sender := newUniformDistributedPeriodicSender(1800, namespace, hubName, provider)
			go sender.Run(runCtx, errChan)

			// Wait for a signal to quit:
			signalChan := make(chan os.Signal, 1)
			signal.Notify(signalChan, os.Interrupt, os.Kill)

			select {
			case <-signalChan:
				log.Println("closing via OS signal...")
				runCancel()
				break
			case err := <-errChan:
				log.Error(err, "failed due to error")
				runCancel()
				break
			}
		},
	}
)

func listenToAll(ctx context.Context, hub *eventhub.Hub, handler eventhub.Handler, errorChan chan error) {
	defer close(errorChan)

	runtimeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	runtimeInfo, err := hub.GetRuntimeInformation(runtimeCtx)
	if err != nil {
		errorChan <- err
		return
	}

	closeHandles := make([]*eventhub.ListenerHandle, len(runtimeInfo.PartitionIDs))
	for idx, partitionID := range runtimeInfo.PartitionIDs {
		startReceiveCtx, startReceiveCancel := context.WithTimeout(context.Background(), 10*time.Second)
		handle, err := hub.Receive(
			startReceiveCtx,
			partitionID,
			handler,
			eventhub.ReceiveWithLatestOffset())
		startReceiveCancel()
		if err != nil {
			errorChan <- err
			return
		}
		closeHandles[idx] = handle
	}

	cases := make([]reflect.SelectCase, len(closeHandles))
	for i, ch := range closeHandles {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch.Done())}
	}
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	log.Println("listening...")
	_, _, _ = reflect.Select(cases)
	log.Println("done listening")
	return
}
