package queue

import (
	"os"
	"encoding/gob"
	"bytes"
	"encoding/binary"
	"deploy-test/logstash/gopath/src/ilog"
)

func AtomicRename(sourceFile, targetFile string) error {
	return os.Rename(sourceFile, targetFile)
}

func Decode(data []byte, to interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(to)
}

func Encode(data interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Int64ToBytes(i int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

// 非数字则返回 -1
func BytesToInt64(bs []byte) (ret int64) {
	ret = -1
	defer func() {
		if x := recover(); x != nil {
			log.Error(bs, "is not int64", x)
		}
	}()
	ret = int64(binary.LittleEndian.Uint64(bs))
	return
}

