package kodo

import (
	"errors"
	"github.com/qiniu/api.v7/auth/qbox"
	"github.com/qiniu/api.v7/storage"
	"strings"

	"golang.org/x/net/context"
	"os"
	"deploy-test/logstash/gopath/src/ilog"
)

var (
	ErrNoSuchBucket = errors.New("no such bucket")
	ErrUploadClosed = errors.New("close upload")
)

type KodoConfig struct {
	Zone      int
	AccessKey string
	SecretKey string
}

type KodoClient struct {
	zone storage.Zone
	cfg  *KodoConfig
	// 暂时通过记录该信息，并外部close file 来关闭上传
	localFiles []*os.File
	closeFlag  bool
}

func canonicalZone(zone int) storage.Zone {
	switch zone {
	case 0:
		return storage.Zone_z0
	case 1:
		return storage.Zone_z1
	case 2:
		return storage.Zone_z2
	case 3:
		return storage.Zone_na0
	case 4:
		return storage.Zone_as0
	default:
		return storage.Zone_z0
	}
}

func NewKodoClient(cfg *KodoConfig) *KodoClient {
	return &KodoClient{
		zone: canonicalZone(cfg.Zone),
		cfg:  cfg,
	}
}

func (cli *KodoClient) ResumeUploaderFile(bucket, key, localFile string, policy storage.PutPolicy) (*storage.PutRet, error) {
	storageConfig := storage.Config{
		Zone:          &cli.zone,
		UseCdnDomains: false,
		UseHTTPS:      false,
	}
	mac := qbox.NewMac(cli.cfg.AccessKey, cli.cfg.SecretKey)
	resumeUploader := storage.NewResumeUploader(&storageConfig)

	policy.Scope = strings.Join([]string{bucket, key}, ":")
	if policy.Expires == 0 {
		policy.Expires = 3600 * 24
	}
	upToken := policy.UploadToken(mac)

	putExtra := storage.RputExtra{
		TryTimes: 3,
	}

	ret := storage.PutRet{}
	if err := resumeUploader.PutFile(context.Background(), &ret, upToken, key, localFile, &putExtra); err != nil {
		if err.Error() == ErrNoSuchBucket.Error() {
			return nil, ErrNoSuchBucket
		}
		return nil, err
	}

	return &ret, nil
}

// 可以通过close file 的当时来中断传输，避免logkit Close 不了
func (cli *KodoClient) ResumeUploader(bucket, key, localFile string, policy storage.PutPolicy) (*storage.PutRet, error) {
	storageConfig := storage.Config{
		Zone:          &cli.zone,
		UseCdnDomains: false,
		UseHTTPS:      false,
	}
	mac := qbox.NewMac(cli.cfg.AccessKey, cli.cfg.SecretKey)
	resumeUploader := storage.NewResumeUploader(&storageConfig)

	policy.Scope = strings.Join([]string{bucket, key}, ":")
	if policy.Expires == 0 {
		policy.Expires = 3600 * 24
	}
	upToken := policy.UploadToken(mac)

	ret := storage.PutRet{}
	f, err := os.Open(localFile)
	if err != nil {
		log.Error("open", localFile, "failed", err)
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		log.Error("get", localFile, "stat fail", err)
		return nil, err
	}
	size := stat.Size()
	cli.localFiles = append(cli.localFiles, f)

	putExtra := storage.RputExtra{
		TryTimes:  3,
		ChunkSize: 1024 * 1024 * 10,
	}

	if cli.closeFlag {
		log.Error("")
		return nil, ErrUploadClosed
	}

	if err := resumeUploader.Put(context.Background(), &ret, upToken, key, f, size, &putExtra); err != nil {
		if cli.closeFlag {
			return nil, ErrUploadClosed
		}
		if err.Error() == ErrNoSuchBucket.Error() {
			return nil, ErrNoSuchBucket
		}
		return nil, err
	}

	return &ret, nil
}

func (cli *KodoClient) Close() {
	cli.closeFlag = true
	for _, file := range cli.localFiles {
		file.Close()
	}
}
