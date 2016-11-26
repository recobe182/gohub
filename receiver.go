package gohub

import (
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	log "github.com/Sirupsen/logrus"
)

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