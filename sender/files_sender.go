package sender

import (
	"sync"
	"github.com/json-iterator/go"
	"github.com/qiniu/logkit/queue"
	"github.com/qiniu/logkit/conf"
	"time"
	"sync/atomic"
	"qiniupkg.com/x/log.v7"
	"os"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/logkit/queue/lk_files"
)

const fileRotateSize = 100 * mb

type fileSender struct {
	stopped     int32
	exitChan    chan struct{}
	innerSender Sender
	filesQueue  queue.FilesQueue
	writeLimit  int // 写入速度限制，单位MB
	strategy    string
	procs       int //发送并发数
	runnerName  string
	saveLogPath string
	stats       StatsInfo
	statsMutex  *sync.RWMutex
	jsontool    jsoniter.API
}

func NewFileSenders(innerSender Sender, conf conf.MapConf, saveLogPath string) (*fileSender, error) {
	logPath, _ := conf.GetStringOr(KeyFtSaveLogPath, saveLogPath)
	syncEvery, _ := conf.GetInt64Or(KeyFtSyncEvery, DefaultFileSyncEver)
	writeLimit, _ := conf.GetIntOr(KeyFtWriteLimit, defaultWriteLimit)
	procs, _ := conf.GetIntOr(KeyFtProcs, defaultMaxProcs)
	runnerName, _ := conf.GetStringOr(KeyRunnerName, UnderfinedRunnerName)
	rotateIntervalSecond, _ := conf.GetIntOr(KeyFileRotateIntervalSyncSecond, 600)
	format, _ := conf.GetStringOr(KeyFileFormatType, "json")
	delim, _ := conf.GetStringOr(KeyFileFormatDelim, ",")
	keyPrefix, _ := conf.GetStringOr(KeyKodoFilePrefix, "")

	err := CreateDirIfNotExist(logPath)
	if err != nil {
		return nil, err
	}

	fileQueue, err := lk_files.NewFilesQueue(runnerName, logPath, keyPrefix, format, delim, fileRotateSize,
		time.Duration(rotateIntervalSecond)*time.Second, fileRotateSize, syncEvery, 5*time.Second, writeLimit*mb)

	if err != nil {
		return nil, err
	}
	sender := fileSender{
		exitChan:    make(chan struct{}),
		procs:       procs,
		innerSender: innerSender,
		filesQueue:  fileQueue,
		writeLimit:  writeLimit,
		saveLogPath: logPath,
		runnerName:  runnerName,
		statsMutex:  new(sync.RWMutex),
		jsontool:    jsoniter.Config{EscapeHTML: true, UseNumber: true}.Froze(),
	}
	sender.asyncFileSendFileFileQueue()
	return &sender, nil
}

func (fs *fileSender) asyncFileSendFileFileQueue() {
	for i := 0; i < fs.procs; i++ {
		go fs.sendFromQueue()
	}
}

func (fs *fileSender) sendFromQueue() {
	timer := time.NewTicker(time.Second * time.Duration(fs.procs))
	for {
		if atomic.LoadInt32(&fs.stopped) > 0 {
			fs.exitChan <- struct{}{}
			return
		}
		logKitFile, ok := fs.filesQueue.PopLogKitFile()
		if !ok {
			continue
		}
		log.Infof("Begin push %s to %s", logKitFile.AbsLocalPath(), logKitFile.AbsDestPath())
		err := fs.innerSender.(FileSender).SendFile(logKitFile)
		if err != nil {
			log.Warnf("send path:%s to %s failed. err: %s, reput it to queue.", logKitFile.AbsLocalPath(), logKitFile.AbsDestPath(), err.Error())
			fs.filesQueue.PutLogKitFile(logKitFile)
			fs.statsMutex.Lock()
			fs.stats.LastError = err.Error()
			fs.statsMutex.Unlock()
		} else {
			log.Infof("Push %s to %s success.", logKitFile.AbsLocalPath(), logKitFile.AbsDestPath())
			fs.filesQueue.FinishLogKitFile(logKitFile)
			fs.statsMutex.Lock()
			fs.stats.Success += int64(logKitFile.WriteCount())
			fs.statsMutex.Unlock()
		}

		select {
		case <-timer.C:
			continue
		}
	}
}

func (fs *fileSender) Send(datas []Data) error {
	se := &StatsError{Ft: true}
	err := fs.filesQueue.PutDatas(datas)
	if err != nil {
		se.ErrorDetail = err
		fs.statsMutex.Lock()
		fs.stats.LastError = err.Error()
		fs.stats.Errors += int64(len(datas))
		fs.statsMutex.Unlock()
	} else {
		se.ErrorDetail = nil
	}
	return se
}

func (fs *fileSender) Name() string {
	return fs.innerSender.Name()
}

func (fs *fileSender) Reset() error {
	return os.RemoveAll(fs.saveLogPath)
}

func (fs *fileSender) Restore(info *StatsInfo) {
	fs.statsMutex.Lock()
	defer fs.statsMutex.Unlock()
	fs.stats = *info
}

func (fs *fileSender) Stats() StatsInfo {
	fs.statsMutex.RLock()
	defer fs.statsMutex.RUnlock()
	return fs.stats
}

func (fs *fileSender) TokenRefresh(mapConf conf.MapConf) (err error) {
	if tokenSender, ok := fs.innerSender.(TokenRefreshable); ok {
		err = tokenSender.TokenRefresh(mapConf)
	}
	return
}

func (fs *fileSender) Close() error {
	atomic.AddInt32(&fs.stopped, 1)
	log.Warnf("Runner[%v] wait for Sender[%v] to completely exit", fs.runnerName, fs.Name())

	// persist queue's meta data
	fs.filesQueue.Close()
	// close，远端文件破损，需要下次重新传，暂没做到续传
	fs.innerSender.Close()
	// 等待正常发送流程退出
	for i := 0; i < fs.procs; i++ {
		<-fs.exitChan
	}
	log.Warnf("Runner[%v] Sender[%v] has been completely exited", fs.runnerName, fs.Name())
	return nil
}
