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
	defaultCheckpointAfter int = 100
)

// ReceiverOption can be passed when creating a receiver to set optional configuration.
type ReceiverOption func(*receiverSetting)

// ReceiverFunc receives messages.
type ReceiverFunc func(msg ReceiveMessage)

type receiverSetting struct {
	partitionId     string
	consumerGroup   string
	checkPointAfter int
	prefetchCount   int
	storageSetting  StorageSetting
}

// ConsumerGroup returns a ReceiverOption that sets consumer group.
func ConsumerGroup(s string) ReceiverOption {
	return func(l *receiverSetting) {
		l.consumerGroup = s
	}
}

// CheckPointAfter returns a ReceiverOption that sets check point after receive a specific amount of messages.
func CheckPointAfter(i int) ReceiverOption {
	return func(l *receiverSetting) {
		l.checkPointAfter = i
	}
}

// PrefetchCount returns a ReceiverOption that sets prefetch count.
func PrefetchCount(i int) ReceiverOption {
	return func(l *receiverSetting) {
		l.prefetchCount = i
	}
}

// ReceiveMessage is a message struct.
type ReceiveMessage struct {
	// Msg is a message body.
	Msg         string

	// Offset is a partition offset.
	Offset      string

	// SeqNo is a message sequence number.
	SeqNo       int64

	PartitionId string

	// Error object is set in case of failure.
	Error       error
}

// EVHReceiver is a receiver interface use to receive a message to Azure Event Hub.
type EVHReceiver interface {
	// Receive a message through out channel.
	// This is a block operation and perform checkpoint right after message is read from this channel.
	Receive(out chan <- ReceiveMessage)

	// Receive a message through out channel.
	// This is a block operation. Checkpoint will be performed after recvFunc is done.
	ReceiveThenWait(recvFunc ReceiverFunc)
}

// EVHReceiver implementation.
type evhReceiver struct {
	receiver        electron.Receiver
	hub             string
	consumerGroup   string
	partitionId     string
	checkpointAfter int
	checkpointCount int

	p               partition
}

func (r*evhReceiver) Receive(out chan <- ReceiveMessage) {
	for {
		if rm, err := r.receiver.Receive(); err == nil {
			ret := ReceiveMessage{
				Msg: string(rm.Message.Body().(amqp.Binary)),
				Offset: rm.Message.Annotations()[offset].(string),
				SeqNo: rm.Message.Annotations()[sequenceNumber].(int64),
				PartitionId: r.partitionId,
				Error: nil,
			}
			log.WithFields(log.Fields{
				"Partition": r.partitionId,
				"Offset": ret.Offset,
				"Seq No": ret.SeqNo,
				"Message": ret.Msg,
			}).Debug("Received")
			out <- ret
			r.checkpoint(&ret)
		} else {
			out <- ReceiveMessage{Error: err}
			break
		}
	}
}

func (r*evhReceiver) ReceiveThenWait(recvFunc ReceiverFunc) {
	for {
		msg := r.getMessage()
		if msg.Error == nil {
			recvFunc(msg)
			r.checkpoint(&msg)
		} else {
			recvFunc(msg)
			break
		}
	}
}

func (r*evhReceiver) getMessage() ReceiveMessage {
	if rm, err := r.receiver.Receive(); err == nil {
		ret := ReceiveMessage{
			Msg: string(rm.Message.Body().(amqp.Binary)),
			Offset: rm.Message.Annotations()[offset].(string),
			SeqNo: rm.Message.Annotations()[sequenceNumber].(int64),
			PartitionId: r.partitionId,
			Error: nil,
		}
		log.WithFields(log.Fields{
			"Partition": r.partitionId,
			"Offset": ret.Offset,
			"Seq No": ret.SeqNo,
			"Message": ret.Msg,
		}).Debug("Received")
		return ret
	} else {
		return ReceiveMessage{Error: err}
	}
}

func (r*evhReceiver) checkpoint(rm *ReceiveMessage) {
	r.checkpointCount++
	if (r.checkpointCount % r.checkpointAfter) == 0 {
		err := r.p.checkpoint(rm)
		if err != nil {
			log.Error(err)
		} else {
			r.checkpointCount = 0
		}
	}
}