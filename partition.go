package gohub

import (
	log "github.com/Sirupsen/logrus"
)


type storage interface {
	createStorage(hub, cg, pid string) error
	getCheckpoint(hub, cg, pid string) (checkpoint, error)
	saveCheckpoint(hub, cg, pid string, cp checkpoint) error
}

type lease struct {
	Offset string `json:"offset"`
	SeqNo int64 `json:"sequenceNumber"`
	PartitionId string `json:"partitionId"`
	Epoch int64 `json:"epoch"`
	Owner string `json:"owner"`
	Token string `json:"token"`
}

type checkpoint struct {
	offset string
	seqNo int64
	partitionId string
}

type partition interface {
	createStorageIfNotExist(hub, cg, pid string) error
	offset(hub, cg, pid string) (string, error)
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
	if m.SeqNo >= cp.seqNo {
		ncp := checkpoint{}
		ncp.offset = m.Offset
		ncp.seqNo = m.SeqNo
		ncp.partitionId = m.PartitionId
		err := p.s.saveCheckpoint(p.hub, p.cg, p.pid, ncp)
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

func (p*defaultPartition) offset(hub, cg, pid string) (string, error) {
	cp, err := p.s.getCheckpoint(hub, cg, pid)
	if err != nil {
		return ``, err
	}
	return cp.offset, err
}

func (p*defaultPartition) createStorageIfNotExist(hub, cg, pid string) error {
	return p.s.createStorage(hub, cg, pid)
}