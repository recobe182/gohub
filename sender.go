package gohub

import (
	"time"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

const (
	partitionKey	string = `x-opt-partition-key`
)

// EVHSender is a sender interface use to send a message to Azure Event Hub.
type EVHSender interface {
	// SendSync sends a message and blocks until the message is acknowledged by the remote receiver.
	// Returns an error or nil in case of success.
	SendSync(body []byte) error

	// SendSyncWithKey act like SendSync. In addition, you can specify partition key. A message with same partition key always sent to same partition.
	SendSyncWithKey(body []byte, pk string) error

	// SendSyncTimeout sends a message and blocks until the message is acknowledged by the remote receiver.
	// If the sending process exceeds the timeout, Error will be returned.
	// Returns an error or nil in case of success.
	SendSyncTimeout(body []byte, t time.Duration) error

	// SendSyncTimeoutWithKey act like SendSyncTimeout. In addition, you can specify partition key. A message with same partition key always sent to same partition.
	SendSyncTimeoutWithKey(body []byte, t time.Duration, pk string) error

	// SendAsync puts a message in the send buffer and returns immediately.
	// If error occurs, an error object will be sent to out channel.
	// Note: can block if there is no space to buffer the message.
	SendAsync(body []byte, out chan <- error)

	// SendAsyncWithKey act like SendAsync. In addition, you can specify partition key. A message with same partition key always sent to same partition.
	SendAsyncWithKey(body []byte, out chan <- error, pk string)

	SendAsyncTimeout(body []byte, out chan <- error, t time.Duration)

	// SendAsyncTimeoutWithKey act like SendAsyncTimeout. In addition, you can specify partition key. A message with same partition key always sent to same partition.
	SendAsyncTimeoutWithKey(body []byte, out chan <- error, t time.Duration, pk string)
}

// EVHSender implementation.
type evhSender struct {
	sender electron.Sender
}

type sendSync func() electron.Outcome
type sendAsync func(out chan <- electron.Outcome)

func (s*evhSender) SendSync(body []byte) error {
	return sendSyncCore(func() electron.Outcome {
		return s.sender.SendSync(getAmqpMessage(body, nil))
	})
}

func (s*evhSender) SendSyncWithKey(body []byte, pk string) error {
	return sendSyncCore(func() electron.Outcome {
		return s.sender.SendSync(getAmqpMessage(body, &pk))
	})
}

func (s*evhSender) SendSyncTimeout(body []byte, t time.Duration) error {
	return sendSyncCore(func() electron.Outcome {
		return s.sender.SendSyncTimeout(getAmqpMessage(body, nil), t)
	})
}

func (s*evhSender) SendSyncTimeoutWithKey(body []byte, t time.Duration, pk string) error {
	return sendSyncCore(func() electron.Outcome {
		return s.sender.SendSyncTimeout(getAmqpMessage(body, &pk), t)
	})
}

func (s*evhSender) SendAsync(body []byte, out chan <- error) {
	go sendAsyncCore(func(o chan <- electron.Outcome) {
		m := getAmqpMessage(body, nil)
		s.sender.SendAsync(m, o, m.Body())
	}, out)
}

func (s*evhSender) SendAsyncWithKey(body []byte, out chan <- error, pk string) {
	go sendAsyncCore(func(o chan <- electron.Outcome) {
		m := getAmqpMessage(body, &pk)
		s.sender.SendAsync(m, o, m.Body())
	}, out)
}

func (s*evhSender) SendAsyncTimeout(body []byte, out chan <- error, t time.Duration) {
	go sendAsyncCore(func(o chan <- electron.Outcome) {
		m := getAmqpMessage(body, nil)
		s.sender.SendAsyncTimeout(m, o, m.Body(), t)
	}, out)
}

func (s*evhSender) SendAsyncTimeoutWithKey(body []byte, out chan <- error, t time.Duration, pk string) {
	go sendAsyncCore(func(o chan <- electron.Outcome) {
		m := getAmqpMessage(body, &pk)
		s.sender.SendAsyncTimeout(m, o, m.Body(), t)
	}, out)
}

func sendSyncCore(s sendSync) error {
	o := s()
	if o.Error != nil {
		return o.Error
	}
	return nil
}

func sendAsyncCore(s sendAsync, out chan <- error) {
	sCh := make(chan electron.Outcome)
	s(sCh)
	o := <- sCh
	out <- o.Error
}

func getAmqpMessage(body []byte, pk *string) amqp.Message{
	m := amqp.NewMessage()
	m.SetInferred(true)
	m.Marshal(body)
	if pk != nil {
		a := make(map[string]interface{})
		a[partitionKey] = *pk
		m.SetAnnotations(a)
	}
	return m
}