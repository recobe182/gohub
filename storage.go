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

// StorageSetting returns setting for Azure Storage.
func StorageSetting(name, key string) storageSetting {
	return storageSetting{name: name, key: key}
}

type storageSetting struct {
	name string
	key string
}

type azureStorage struct {
	ss storageSetting
	c azure.Client
}

func newAzureStorage(ss storageSetting) *azureStorage {
	c, err := azure.NewClient(
		ss.name,
		ss.key,
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

func (s*azureStorage) createStorage(hub, cg, pid string) error {
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

func (s*azureStorage) getCheckpoint(hub, cg, pid string) (checkpoint, error) {
	l, err := s.getLease(hub, cg, pid)
	if err != nil {
		return checkpoint{}, err
	}
	log.WithFields(log.Fields{
		"Offset": l.Offset,
		"SeqNo": l.SeqNo,
		"PartitionID": l.PartitionId},
	).Debug("Got latest offset")
	return checkpoint{offset: l.Offset, seqNo: l.SeqNo, partitionId: l.PartitionId}, nil
}

func (s*azureStorage) saveCheckpoint(hub, cg, pid string, cp checkpoint) error {
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
	l.Offset = "0"
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