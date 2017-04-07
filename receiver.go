package gohub

import (
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

const (
	offsetKey 		string = `x-opt-offset`
	sequenceNumberKey 	string = `x-opt-sequence-number`
	defaultConsumerGroup 	string = `$Default`
	defaultPrefetchCount 	int = 999
)

type ReceiveMode string

// ReceiverOption can be passed when creating a receiver to set optional configuration.
type ReceiverOption func(*receiverSetting)

type receiverSetting struct {
	partitionId		string
	consumerGroup		string
	prefetchCount		int
	epochTimeInMillisec	*int64
	storageSetting		*StorageSetting
}

// ConsumerGroup returns a ReceiverOption that sets consumer group.
func ConsumerGroup(s string) ReceiverOption { return func(l *receiverSetting) { l.consumerGroup = s } }

// PrefetchCount returns a ReceiverOption that sets prefetch count.
func PrefetchCount(i int) ReceiverOption { return func(l *receiverSetting) { l.prefetchCount = i } }

// FromTime returns a ReceiverOption that make this receiver start receive message from specific time.
func FromTime(epochTimeInMillisec int64) ReceiverOption {
	return func(l *receiverSetting) {
		l.epochTimeInMillisec = &epochTimeInMillisec
	}
}

// FromLastOffset returns a ReceiverOption that make this receiver start receive message last offset.
func FromLastOffset(ss StorageSetting) ReceiverOption {
	return func(l *receiverSetting) {
		l.storageSetting = &StorageSetting{
			Name: ss.Name,
			Key: ss.Key,
		}
	}
}

// ReceiveMessage is a message struct.
type ReceiveMessage struct {
	// Msg is a message body.
	Body []byte

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
	Receive(out chan <- ReceiveMessage) PartitionContext
}

// EVHReceiver implementation.
type evhReceiver struct {
	receiver electron.Receiver
	hub string
	consumerGroup string
	partitionId string

	p PartitionContext
}

func (r*evhReceiver) Receive(out chan <- ReceiveMessage) PartitionContext {
	go func() {
		for {
			if rm, err := r.receiver.Receive(); err != nil {
				out <- ReceiveMessage{Error: err}
				break
			} else {
				var body []byte
				if rm.Message.Body() != nil {
					body = []byte(rm.Message.Body().(amqp.Binary))
				}

				ret := ReceiveMessage{
					Body: body,
					Offset: rm.Message.Annotations()[offsetKey].(string),
					SeqNo: rm.Message.Annotations()[sequenceNumberKey].(int64),
					PartitionId: r.p.GetId(),
					Error: nil,
				}
				out <- ret
			}
		}
	}()
	return r.p
}