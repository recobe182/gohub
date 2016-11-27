package gohub

import (
	log "github.com/Sirupsen/logrus"
)


type storage interface {
	getCheckpoint(hub, cg, pid string) (*checkpoint, error)
	saveCheckpoint(cp *checkpoint) error
}

type checkpoint struct {
	Offset string `json:"offset"`
	SeqNo int64 `json:"sequenceNumber"`
	PartitionId string `json:"partitionId"`
}

type partition interface {
	checkpoint(m *ReceiveMessage) error
}

type defaultPartition struct {
	hub string
	cg string
	pid string
	s storage
}

func newPartition(hub, cg, pid string, s storage) *defaultPartition {
	p := &defaultPartition{hub, cg, pid, s}
	return p
}

func (p*defaultPartition) checkpoint(m *ReceiveMessage) error {
	cp, err := p.s.getCheckpoint(p.hub, p.cg, p.pid)
	if err != nil {
		log.Error("Unable to get checkpoint.")
		return err
	}
	log.WithFields(log.Fields{
		"Partition": cp.PartitionId,
		"Offset": cp.Offset,
		"SeqNo": cp.SeqNo,
	}).Debug("Retrieved checkpoint")
	if m.SeqNo >= cp.SeqNo {
		err := p.s.saveCheckpoint(cp)
		if err != nil {
			log.Error("Unable to save checkpoint.")
			return err
		}
		log.WithFields(log.Fields{
			"Partition": m.PartitionId,
			"Offset": m.Offset,
			"SeqNo": m.SeqNo,
		}).Debug("Checkpointed")
	}
	return nil
}
