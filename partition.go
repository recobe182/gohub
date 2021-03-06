package gohub

// PartitionContext is for doing checkpoint.
// If receiver mode is FromNow, the checkpoint operation will do nothing.
// If receiver mode is FromLastOffset, you must provide storage account setting to make checkpoint works.
type PartitionContext interface {
	Checkpoint(offset string, seqNo int64) error
	GetId() string
}

type partitionContext struct {
	hub string
	cg string
	pid string
	s storage
}

func newPartitionContext(hub, cg, pid string, s storage) *partitionContext {
	p := &partitionContext{hub, cg, pid, s}
	return p
}

func (p*partitionContext) Checkpoint(offset string, seqNo int64) error {
	if p.s != nil {
		cp, err := p.s.GetCheckpoint(p.hub, p.cg, p.pid)
		if err != nil {
			return err
		}
		if seqNo >= cp.seqNo {
			ncp := checkpoint{}
			ncp.offset = offset
			ncp.seqNo = seqNo
			err := p.s.SaveCheckpoint(p.hub, p.cg, p.pid, ncp)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p*partitionContext) GetId() string {
	return p.pid
}