package v1

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

type BackupStorageType string

const (
	BackupStorageFilesystem BackupStorageType = "filesystem"
	BackupStorageS3         BackupStorageType = "s3"
	BackupStorageAzure      BackupStorageType = "azure"
)

type BackupStorageS3Spec struct {
	Bucket            string                    `json:"bucket"`
	CredentialsSecret string                    `json:"credentialsSecret"`
	Region            string                    `json:"region,omitempty"`
	EndpointURL       string                    `json:"endpointUrl,omitempty"`
	CABundle          *corev1.SecretKeySelector `json:"caBundle,omitempty"`
}

// BucketAndPrefix returns bucket name and backup prefix from Bucket.
// BackupStorageS3Spec.Bucket can contain backup path in format `<bucket-name>/<backup-prefix>`.
func (b *BackupStorageS3Spec) BucketAndPrefix() (string, string) {
	bucket, prefix, _ := strings.Cut(b.Bucket, "/")

	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/")
		prefix += "/"
	}

	return bucket, prefix
}

type BackupStorageAzureSpec struct {
	CredentialsSecret string `json:"credentialsSecret"`
	ContainerPath     string `json:"container"`
	Endpoint          string `json:"endpointUrl"`
	StorageClass      string `json:"storageClass"`
	BlockSize         int64  `json:"blockSize"`
	Concurrency       int    `json:"concurrency"`
}
