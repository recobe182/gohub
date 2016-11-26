package gohub

import (
	"time"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	log "github.com/Sirupsen/logrus"
)

const (
	utf_8 string = `UTF-8`
)

// EVHSender is a sender interface use to send a message to Azure Event Hub.
type EVHSender interface {
	// SendSync sends a message and blocks until the message is acknowledged by the remote receiver.
	// Returns an error or nil in case of success.
	SendSync(msg string) error

	// SendSyncTimeout sends a message and blocks until the message is acknowledged by the remote receiver.
	// If the sending process exceeds the timeout, Error will be returned.
	// Returns an error or nil in case of success.
	SendSyncTimeout(msg string, t time.Duration) error

	// SendAsync puts a message in the send buffer and returns immediately.
	// If error occurs, an error object will be sent to out channel.
	// Note: can block if there is no space to buffer the message.
	SendAsync(msg string, out chan <- error)

	SendAsyncTimeout(msg string, out chan <- error, t time.Duration)
}

// EVHSender implementation.
type evhSender struct {
	sender electron.Sender
}

type sendSync func() electron.Outcome
type sendAsync func(out chan <- electron.Outcome)

func (s*evhSender) SendSync(msg string) error {
	return sendSyncCore(func() electron.Outcome {
		return s.sender.SendSync(getAmqpMessage(msg))
	})
}

func (s*evhSender) SendSyncTimeout(msg string, t time.Duration) error {
	return sendSyncCore(func() electron.Outcome {
		return s.sender.SendSyncTimeout(getAmqpMessage(msg), t)
	})
}

func (s*evhSender) SendAsync(msg string, out chan <- error) {
	go sendAsyncCore(func(o chan <- electron.Outcome) {
		m := getAmqpMessage(msg)
		s.sender.SendAsync(m, o, m.Body())
	}, out)
}

func (s*evhSender) SendAsyncTimeout(msg string, out chan <- error, t time.Duration) {
	go sendAsyncCore(func(o chan <- electron.Outcome) {
		m := getAmqpMessage(msg)
		s.sender.SendAsyncTimeout(m, o, m.Body(), t)
	}, out)
}

func sendSyncCore(s sendSync) error {
	o := s()
	if o.Error != nil {
		return o.Error
	}
	log.WithFields(log.Fields{"Status": o.Status, "Value": o.Value}).Debug("Sent SYNC")
	return nil
}

func sendAsyncCore(s sendAsync, out chan <- error) {
	sCh := make(chan electron.Outcome)
	s(sCh)
	o := <- sCh
	log.WithFields(log.Fields{"Status": o.Status, "Value": o.Value}).Debug("Sent ASYNC")
	out <- o.Error
}

func getAmqpMessage(msg string) amqp.Message{
	m := amqp.NewMessage()
	m.SetInferred(true)
	m.SetContentEncoding(utf_8)
	m.Marshal([]byte(msg))
	return m
}