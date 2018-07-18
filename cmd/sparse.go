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

	time.Sleep(20 * time.Second) // initial delay to wait for listening to begin

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// send a pair of messages on the same link spread across a variable amount of time
			err := u.sendPair(ctx)
			if err != nil {
				errChan <- err
				return
			}
		}
	}
}

func (u *uniformDistributedPeriodicSender) sendPair(ctx context.Context) error {
	// send a pair of messages on the same link spread across a variable amount of time
	hub, err := eventhub.NewHub(u.namespace, u.hubName, u.tokenProvider, eventhub.HubWithEnvironment(environment()))
	if err != nil {
		return err
	}

	err = u.send(ctx, hub)
	if err != nil {
		return err
	}

	delay := time.Duration(rand.Intn(u.maxSeconds)) * time.Second
	log.Printf("next send at: %q", time.Now().Add(delay).Format("2006-01-02 15:04:05"))
	time.Sleep(delay)

	err = u.send(ctx, hub)
	if err != nil {
		return err
	}

	err = hub.Close(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (u *uniformDistributedPeriodicSender) send(ctx context.Context, hub *eventhub.Hub) error {
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}
	event1 := eventhub.NewEvent([]byte(id.String()))
	event1.ID = id.String()
	err = hub.Send(ctx, event1)
	if err != nil {
		return err
	}
	log.Printf("Sent: %q", event1.ID)
	return nil
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
				log.Error(err)
				log.Error(runCtx.Err())
				runCancel()
				break
			}
		},
	}
)

func listenToAll(ctx context.Context, hub *eventhub.Hub, handler eventhub.Handler, errorChan chan error) {
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
	chosen, recv, ok := reflect.Select(cases)
	log.Println(chosen, recv, ok)
	log.Println("done listening")
	return
}
