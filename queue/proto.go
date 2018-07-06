package queue

import (
	. "github.com/qiniu/logkit/utils/models"
)

// BackendQueue represents the behavior for the secondary message
// storage system
type BackendQueue interface {
	Name() string
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

type LogKitFile interface {
	Remove() error
	AbsLocalPath() string
	AbsDestPath() string
	Close() error
	WriteCount() int
	WritePos() int64
}
type FilesQueue interface {
	Name() string
	PutFile(localPath, destPath string, writeCount int, needRemove bool) (err error)
	PutLogKitFile(LogKitFile) error
	PutDatas([] Data) error
	FinishLogKitFile(LogKitFile) error
	PopLogKitFile() (LogKitFile, bool)
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

// DataQueue 代表了无需编解码可直接放取 Data 的队列
type DataQueue interface {
	// PutDatas 用于存放一组数据
	PutDatas([]Data) error
	// ReadDatasChan 用于获取直接读取 Data 的管道
	ReadDatasChan() <-chan []Data
}

const (
	FROM_NONE   = iota
	FROM_DISK
	FROM_MEMORY
)
