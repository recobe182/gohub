package gohub

import (
	"encoding/json"
	"io/ioutil"
	azure "github.com/Azure/azure-sdk-for-go/storage"
	"crypto/md5"
	"encoding/base64"
	"bytes"
	log "github.com/Sirupsen/logrus"
)

const (
	contentMd5 string = `Content-MD5`
)

type StorageSetting struct {
	Name string
	Key string
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
}


type storage interface {
	CreateStorage(hub, cg, pid string) error
	GetCheckpoint(hub, cg, pid string) (checkpoint, error)
	SaveCheckpoint(hub, cg, pid string, cp checkpoint) error
}

type azureStorage struct {
	ss StorageSetting
	c azure.Client
}

func newAzureStorage(ss StorageSetting) *azureStorage {
	c, err := azure.NewClient(
		ss.Name,
		ss.Key,
		azure.DefaultBaseURL,
		azure.DefaultAPIVersion,
		false,
	)
	if err != nil {
		panic(err)
	}
	a := &azureStorage{
		c: c,
		ss: ss,
	}
	return a
}

func (s*azureStorage) CreateStorage(hub, cg, pid string) error {
	bs := s.c.GetBlobService()
	_, err := bs.CreateContainerIfNotExists(hub, azure.ContainerAccessTypeBlob)
	if err != nil {
		return err
	}
	bn := getBlobName(cg, pid)
	exists, err := bs.BlobExists(hub, bn)
	if err != nil {
		return err
	}
	if exists {
		log.WithFields(log.Fields{"Hub": hub, "ConsumerGroup": cg, "PartitionID": pid}).Debug("Lease Exists")
		return nil
	}
	if err = bs.CreateBlockBlob(hub, bn); err != nil {
		return err
	}
	if err = s.createNewLease(hub, cg, pid); err != nil {
		return err
	}
	return nil
}

func (s*azureStorage) GetCheckpoint(hub, cg, pid string) (checkpoint, error) {
	l, err := s.getLease(hub, cg, pid)
	if err != nil {
		return checkpoint{}, err
	}
	if l.Offset == "" {
		l.Offset = "-1"
	}
	log.WithFields(log.Fields{
		"Offset": l.Offset,
		"SeqNo": l.SeqNo,
		"PartitionID": l.PartitionId},
	).Debug("Got latest offset")
	return checkpoint{offset: l.Offset, seqNo: l.SeqNo}, nil
}

func (s*azureStorage) SaveCheckpoint(hub, cg, pid string, cp checkpoint) error {
	l, err := s.getLease(hub, cg, pid)
	if err != nil {
		return nil
	}
	l.Offset = cp.offset
	l.SeqNo = cp.seqNo
	if err := s.saveLease(hub, cg, pid, l); err != nil {
		return err
	}
	return nil
}

func (s*azureStorage) getLease(hub, cg, pid string) (lease, error) {
	var l lease
	bs := s.c.GetBlobService()
	r, err := bs.GetBlob(hub, getBlobName(cg, pid))
	defer r.Close()
	if err != nil {
		return l, err
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return l, err
	}
	if err = json.Unmarshal(b, &l); err != nil {
		return l, err
	}
	return l, nil
}

func (s*azureStorage) createNewLease(hub, cg, pid string) error {
	l := lease{}
	l.Offset = "-1"
	l.SeqNo = 0
	l.Epoch = 0
	l.PartitionId = pid
	l.Owner = `gohub`
	l.Token = ``
	if err := s.saveLease(hub, cg, pid, l); err != nil {
		return err
	}
	log.Debug("New Lease Created")
	return nil
}

func (s*azureStorage) saveLease(hub, cg, pid string, l lease) error {
	b, err := json.Marshal(l)
	if err != nil {
		return err
	}

	h := md5.New()
	h.Write(b)
	md5 := base64.StdEncoding.EncodeToString(h.Sum(nil))

	headers := make(map[string]string)
	headers[contentMd5] = md5

	bn := getBlobName(cg, pid)
	bs := s.c.GetBlobService()
	if err := bs.PutBlockWithLength(hub, bn, md5, uint64(len(b)), bytes.NewReader(b), headers); err != nil {
		return err
	}
	if err := bs.PutBlockList(hub, bn, []azure.Block{{ID: md5, Status: azure.BlockStatusLatest}}); err != nil {
		return err
	}
	return nil
}

func getBlobName(cg, pid string) string {
	return cg + "/" + pid
}