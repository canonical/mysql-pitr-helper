package storage

import api "mysql-pitr-helper/pkg/apis/pxc/v1"

type Options interface {
	Type() api.BackupStorageType
}

type S3Options struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	BucketName      string
	Prefix          string
	Region          string
	VerifyTLS       bool
	CABundle        []byte
}

func (o *S3Options) Type() api.BackupStorageType {
	return api.BackupStorageS3
}

type AzureOptions struct {
	StorageAccount string
	AccessKey      string
	Endpoint       string
	Container      string
	Prefix         string
	BlockSize      int64
	Concurrency    int
}

func (o *AzureOptions) Type() api.BackupStorageType {
	return api.BackupStorageAzure
}
