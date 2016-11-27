package gohub

import (
	"encoding/json"
	"io/ioutil"
	azure "github.com/Azure/azure-sdk-for-go/storage"
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

func (s*azureStorage) getCheckpoint(hub, cg, pid string) (*checkpoint, error) {
	bs := s.c.GetBlobService()
	r, err := bs.GetBlob(hub, cg + "/" + pid)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var cp checkpoint
	err = json.Unmarshal(b, &cp)
	if err != nil {
		return nil, err
	}
	return &cp, nil
}

func (s*azureStorage) saveCheckpoint(cp *checkpoint) error {
	return nil
}