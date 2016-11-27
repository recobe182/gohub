package gohub

import (
	"testing"
	"errors"
)

type storageStub struct {
	b bool
	gf bool
	sf bool
}

func (s*storageStub) getCheckpoint(hub, cg, pid string) (*checkpoint, error) {
	if s.gf {
		return nil, errors.New(`ERROR`)
	}
	return &checkpoint{SeqNo: 10}, nil
}

func (s*storageStub) saveCheckpoint(cp *checkpoint) error {
	s.b = false
	if s.sf {
		return errors.New(`ERROR`)
	}
	s.b = true
	return nil
}

func TestPartitionCheckpointSuccess(t *testing.T) {
	s := &storageStub{false, false, false}
	p := newPartition(`hub`, `cg`, `pid`, s)
	cs := []struct {
		in *ReceiveMessage
		cp bool
	} {
		{&ReceiveMessage{SeqNo: 3}, false},
		{&ReceiveMessage{SeqNo: 13}, true},
	}
	for _, c := range cs {
		p.checkpoint(c.in)
		if s.b != c.cp {
			t.Errorf(`Result mismatched %v %v`, s.b, c.cp)
		}
	}
}

func TestPartitionCheckpointGetFailure(t *testing.T) {
	s := &storageStub{false, true, false}
	p := newPartition(`hub`, `cg`, `pid`, s)
	cs := []struct {
		in *ReceiveMessage
	} {
		{&ReceiveMessage{}},
	}
	for _, c := range cs {
		got := p.checkpoint(c.in)
		if got == nil {
			t.Errorf(`Expected Error but got nil`)
		}
	}
}

func TestPartitionCheckpointSaveFailure(t *testing.T) {
	s := &storageStub{false, false, true}
	p := newPartition(`hub`, `cg`, `pid`, s)
	cs := []struct {
		in *ReceiveMessage
		cp bool
	} {
		{&ReceiveMessage{`Bee`, `1`, 20, `1`, nil}, true},
	}
	for _, c := range cs {
		got := p.checkpoint(c.in)
		if got == nil {
			t.Errorf(`Expected Error but got nil`)
		}
	}
}
