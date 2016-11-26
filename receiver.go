package gohub

import (
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	log "github.com/Sirupsen/logrus"
)

// ReceiverOption can be passed when creating a receiver to set optional configuration.
type ReceiverOption func(*receiverSetting)

type receiverSetting struct {
	partition int

	consumerGroup string
	checkPointAfter int
	prefetchCount int
}

// ConsumerGroup returns a ReceiverOption that sets consumer group.
func ConsumerGroup(s string) ReceiverOption { return func(l *receiverSetting) { l.consumerGroup = s } }

// CheckPointAfter returns a ReceiverOption that sets check point after receive a specific amount of messages.
func CheckPointAfter(i int) ReceiverOption { return func(l *receiverSetting) { l.checkPointAfter = i } }

// PrefetchCount returns a ReceiverOption that sets prefetch count.
func PrefetchCount(i int) ReceiverOption { return func(l *receiverSetting) { l.prefetchCount = i } }

// ReceiveMessage is a message struct.
type ReceiveMessage struct {
	// Msg is a message body.
	Msg string

	// Error object is set in case of failure.
	Error error
}

// EVHReceiver is a receiver interface use to receive a message to Azure Event Hub.
type EVHReceiver interface {
	// Receive a message through out channel.
	Receive(out chan <- ReceiveMessage)
}

// EVHReceiver implementation.
type evhReceiver struct {
	receiver electron.Receiver
	partition int
}

func (r*evhReceiver) Receive(out chan <- ReceiveMessage) {
	for {
		if rm, err := r.receiver.Receive(); err != nil {
			out <- ReceiveMessage{Error: err}
			break
		} else {
			msg := string(rm.Message.Body().(amqp.Binary))
			log.WithFields(log.Fields{"Partition": r.partition, "Message": msg}).Debug("Received")
			out <- ReceiveMessage{Msg: msg, Error: nil}
		}
	}
}