package gohub

import (
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	log "github.com/Sirupsen/logrus"
)

const (
	offset string = `x-opt-offset`
	sequenceNumber string = `x-opt-sequence-number`
	defaultConsumerGroup string = `$Default`
	defaultPrefetchCount int = 999
)

// ReceiverOption can be passed when creating a receiver to set optional configuration.
type ReceiverOption func(*receiverSetting)

type receiverSetting struct {
	partitionId string
	consumerGroup string
	prefetchCount int
	storageSetting StorageSetting
}

// ConsumerGroup returns a ReceiverOption that sets consumer group.
func ConsumerGroup(s string) ReceiverOption { return func(l *receiverSetting) { l.consumerGroup = s } }

// PrefetchCount returns a ReceiverOption that sets prefetch count.
func PrefetchCount(i int) ReceiverOption { return func(l *receiverSetting) { l.prefetchCount = i } }

// ReceiveMessage is a message struct.
type ReceiveMessage struct {
	// Msg is a message body.
	Msg string

	// Offset is a partition offset.
	Offset string

	// SeqNo is a message sequence number.
	SeqNo int64

	PartitionId string

	// Error object is set in case of failure.
	Error error
}

// EVHReceiver is a receiver interface use to receive a message to Azure Event Hub.
type EVHReceiver interface {
	// Receive a message through out channel.
	Receive(out chan <- ReceiveMessage)

	Checkpoint(msg ReceiveMessage) error
}

// EVHReceiver implementation.
type evhReceiver struct {
	receiver electron.Receiver
	hub string
	consumerGroup string
	partitionId string

	p PartitionContext
}

func (r*evhReceiver) Receive(out chan <- ReceiveMessage) {
	for {
		if rm, err := r.receiver.Receive(); err != nil {
			out <- ReceiveMessage{Error: err}
			break
		} else {
			ret := ReceiveMessage{
				Msg: string(rm.Message.Body().(amqp.Binary)),
				Offset: rm.Message.Annotations()[offset].(string),
				SeqNo: rm.Message.Annotations()[sequenceNumber].(int64),
				PartitionId: r.p.GetId(),
				Error: nil,
			}
			log.WithFields(log.Fields{
				"Partition": r.partitionId,
				"Offset": ret.Offset,
				"Seq No": ret.SeqNo,
				"Message": ret.Msg,
			}).Debug("Received")
			out <- ret
		}
	}
}

func (r*evhReceiver) Checkpoint(msg ReceiveMessage) error {
	return r.p.Checkpoint(msg.Offset, msg.SeqNo)
}