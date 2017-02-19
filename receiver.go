package gohub

import (
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	log "github.com/Sirupsen/logrus"
)

const (
	offsetKey 		string = `x-opt-offset`
	sequenceNumberKey 	string = `x-opt-sequence-number`
	defaultConsumerGroup 	string = `$Default`
	defaultPrefetchCount 	int = 999

	FromLastOffset		ReceiveMode = "FromLastOffset"
	FromNow			ReceiveMode = "FromNow"
)

type ReceiveMode string

// ReceiverOption can be passed when creating a receiver to set optional configuration.
type ReceiverOption func(*receiverSetting)

type receiverSetting struct {
	partitionId	string
	consumerGroup	string
	prefetchCount	int
	storageSetting	*StorageSetting
	mode		ReceiveMode
}

// ConsumerGroup returns a ReceiverOption that sets consumer group.
func ConsumerGroup(s string) ReceiverOption { return func(l *receiverSetting) { l.consumerGroup = s } }

// PrefetchCount returns a ReceiverOption that sets prefetch count.
func PrefetchCount(i int) ReceiverOption { return func(l *receiverSetting) { l.prefetchCount = i } }

// Mode returns a ReceiverOption that sets receiver mode. This can be FromLastOffset or FromNow.
func Mode(mode ReceiveMode) ReceiverOption {
	return func(l *receiverSetting) {
		l.mode = mode
	}
}

// StorageAccount returns a ReceiverOption that Azure Storage Account. If you use FromLastOffset mode, this is mandatory.
func StorageAccount(ss StorageSetting) ReceiverOption {
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
				ret := ReceiveMessage{
					Msg: string(rm.Message.Body().(amqp.Binary)),
					Offset: rm.Message.Annotations()[offsetKey].(string),
					SeqNo: rm.Message.Annotations()[sequenceNumberKey].(int64),
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
	}()
	return r.p
}