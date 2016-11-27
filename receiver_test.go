package gohub

import (
	"testing"
)

type stubPartition struct {
	b bool
}

func (s*stubPartition) checkpoint(m *ReceiveMessage) error {
	s.b = true
	return nil
}


func TestReceiverCheckpoint(t *testing.T) {
	p := &stubPartition{b: false}
	r := evhReceiver{
		checkpointAfter: 2,
		p: p,
	}
	r.checkpoint(&ReceiveMessage{})
	r.checkpoint(&ReceiveMessage{})
	if p.b == false {
		t.Errorf(`checkpoint not reached`)
	}
}