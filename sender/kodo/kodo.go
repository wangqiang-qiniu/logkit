package kodo

import (
	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"qiniupkg.com/x/errors.v7"
	"github.com/qiniu/api.v7/storage"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/logkit/queue"
	"fmt"
)

type KodoSender struct {
	Bucket    string
	KeyPrefix string
	name      string
	Zone      int
	AccessKey string
	SecretKey string

	client *KodoClient
}

func init() {
	sender.RegisterConstructor(sender.TypeKodo, NewSender)
}

// kodo sender
func NewSender(conf conf.MapConf) (kodoSender sender.Sender, err error) {
	ak, _ := conf.GetString(sender.KeyKodoAK)
	akFromEnv := GetEnv(ak)
	if akFromEnv == "" {
		akFromEnv = ak
	}

	sk, _ := conf.GetString(sender.KeyKodoSK)
	skFromEnv := GetEnv(sk)
	if skFromEnv == "" {
		skFromEnv = sk
	}

	bucket, _ := conf.GetStringOr(sender.KeyKodoBucketName, "")
	keyPrefix, _ := conf.GetStringOr(sender.KeyKodoFilePrefix, "")
	zoneStr, _ := conf.Get(sender.KeyKodoZone)
	name, _ := conf.GetStringOr(sender.KeyName, fmt.Sprintf("kodoSender:(Bucket:%s,Prefix:%s)", bucket, keyPrefix))

	zone := 0
	switch zoneStr {
	case "华东":
		zone = 0
	case "华北":
		zone = 1
	case "华南":
		zone = 2
	case "北美":
		zone = 3
	case "东南亚":
		zone = 4
	}

	if len(bucket) == 0 {
		return nil, errors.New("bucket is not null")
	}
	sender := KodoSender{
		Bucket:    bucket,
		KeyPrefix: keyPrefix,
		Zone:      zone,
		name:      name,

		AccessKey: akFromEnv,
		SecretKey: skFromEnv,
		client: NewKodoClient(&KodoConfig{
			Zone:      zone,
			AccessKey: akFromEnv,
			SecretKey: skFromEnv,
		})}
	return &sender, nil
}

func (this *KodoSender) Name() string {
	return this.name
}

func (this *KodoSender) Send(data []Data) error {
	// 暂时没开发，后续如果可能，可以支持append的样式，将数据直接发送到 kodo 的object
	return nil
}

func (ks *KodoSender) SendFile(file queue.LogKitFile) error {
	policy := storage.PutPolicy{}
	_, err := ks.client.ResumeUploader(ks.Bucket, file.AbsDestPath(), file.AbsLocalPath(), policy)
	return err
}

func (ks *KodoSender) Close() (err error) {
	log.Infof("kodo sender was closed")
	ks.client.Close()
	return nil
}
