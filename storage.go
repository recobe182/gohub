package gohub

type storageSetting struct {
	name string
	key string
}

// StorageSetting returns setting for Azure Storage.
func StorageSetting(name, key string) storageSetting {
	return storageSetting{name: name, key: key}
}