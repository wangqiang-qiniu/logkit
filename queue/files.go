package queue

import (
	"sync"
	"time"
	"os"
	"fmt"
	"io"
	"path"
	"bufio"
	"github.com/qiniu/log"
	"math/rand"
	"qiniupkg.com/x/errors.v7"
	"github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/logkit/queue/slice"
	"github.com/syndtr/goleveldb/leveldb"
)

/**
	文件来源: 1. PutData：接收数据，并按照规则保存文件分片，文件分片
			 2. PutFile：
	文件 -> waitSchedQueue -> fileSliceChan(fileSliceChanMap)
 */

type fileQueue struct {
	writeFileNum int64 // 当前文件的index
	writeCount   int   // 当前文件写入的条数
	count        int64 //写入次数，根据该数字去持久化元数据
	sync.RWMutex

	// instantiaion time metadata
	name                     string
	fileRotateSize           int64         // NOTE: currently this cannot change once created
	fileRotateIntervalSecond time.Duration //多长时间强制发送一次数据
	syncEveryWrite           int64         // number of writes per fsync
	syncMetaTimeout          time.Duration // duration of time per fsync
	exitFlag                 int32
	needSync                 bool
	maxMsgSize               int
	currFileSlice            *slice.FileSlice
	dataPath                 string
	format                   string
	delim                    string
	writeLimit               int
	ldb                      *leveldb.DB

	// internal channels
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int
	fileSliceChan     chan slice.FileSlice       // 提供给外部获取上传文件的chan, 相当于已经被调度的上传文件或者文件分片
	waitSchedQueue    [] slice.FileSlice         // 主要是防止fileMetaChan没有空间，阻塞队列
	fileSliceChanMap  map[string]slice.FileSlice // fileMetaChan 里的代上传文件，方便持久化，重启的时候恢复
}

func NewFilesQueue(name string, dataPath string, format, delim string, fileRotateSize int64,
	fileRotateIntervalSecond time.Duration, maxMsgSize int, syncEveryWrite int64,
	syncMetaTimeout time.Duration, writeLimit int) FilesQueue {

	fq := fileQueue{
		name:                     name,
		dataPath:                 dataPath,
		fileRotateSize:           fileRotateSize,
		fileRotateIntervalSecond: fileRotateIntervalSecond,
		emptyChan:                make(chan int),
		emptyResponseChan:        make(chan error),
		exitChan:                 make(chan int),
		exitSyncChan:             make(chan int),
		syncEveryWrite:           syncEveryWrite,
		maxMsgSize:               maxMsgSize,
		format:                   format,
		delim:                    delim,
		writeLimit:               writeLimit,
		syncMetaTimeout:          syncMetaTimeout,
		fileSliceChan:            make(chan slice.FileSlice, 20),
		waitSchedQueue:           []slice.FileSlice{},
		fileSliceChanMap:         make(map[string]slice.FileSlice),
	}
	err := fq.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		log.Warnf("ERROR: filequeue(%s) failed to retrieveMetaData - %s", fq.name, err)
	}
	go fq.ioLoop()
	return &fq
}

// Name returns the name of the queue
func (fq *fileQueue) Name() string {
	return fq.name
}

func (fq *fileQueue) Depth() int64 {
	fq.RLock()
	defer fq.RUnlock()
	return int64(len(fq.fileSliceChan) + len(fq.waitSchedQueue))
}

// Close cleans up the queue and persists metadata
func (fq *fileQueue) Close() error {
	err := fq.exit(false)
	if err != nil {
		return err
	}
	fq.persistQueueData()
	fq.sync()

	return nil
}

func (fq *fileQueue) Delete() error {
	return fq.exit(true)
}

func (fq *fileQueue) exit(deleted bool) error {
	fq.Lock()
	defer fq.Unlock()

	fq.exitFlag = 1

	if deleted {
		log.Warnf("FILEQUEUE(%s): deleting", fq.name)
	} else {
		log.Warnf("FILEQUEUE(%s): closing", fq.name)
	}
	close(fq.fileSliceChan)
	close(fq.exitChan)
	// ensure that ioLoop has exited
	<-fq.exitSyncChan

	if fq.currFileSlice != nil {
		fq.currFileSlice.Close()
	}
	return nil
}

func (fq *fileQueue) PopFileSlice() slice.FileSlice {
	return <-fq.fileSliceChan
}

// 选择 waitSchedQueue 队列，加入 fileSliceChan
func (fq *fileQueue) schedFile2FileSliceChan() {
	fq.RLock()
	defer fq.RUnlock()
	if len(fq.waitSchedQueue) == 0 {
		return
	}
	if len(fq.fileSliceChan) < cap(fq.fileSliceChan) {
		file := fq.waitSchedQueue[0]
		fq.fileSliceChan <- file
		fq.fileSliceChanMap[file.FileAbsPath()] = file
		fq.waitSchedQueue = fq.waitSchedQueue[1:]
		log.Debug(file, " scheduler to fileSliceChan")
	}
}

func (fq *fileQueue) putFile2WaitSchQueue(slice slice.FileSlice) error {
	fq.RLock()
	defer fq.RUnlock()
	if fq.exitFlag == 1 {
		return errors.New("exiting")
	}
	_, err := os.Stat(slice.FileAbsPath())
	if err != nil {
		log.Warn("Put file failed.", err)
		return err
	}
	fq.waitSchedQueue = append(fq.waitSchedQueue, slice)
	fq.persistQueueData()
	return nil
}

func (fq *fileQueue) finishFile(slice *slice.FileSlice) {
	fq.RLock()
	defer fq.RUnlock()
	if v, ok := fq.fileSliceChanMap[slice.FileAbsPath()]; ok {
		log.Debugf("%s finish", v)
		delete(fq.fileSliceChanMap, v.FileAbsPath())
		v.Remove()
		fq.persistQueueData()
	}
}

// Put 文件去上传
func (fq *fileQueue) PutFile(slice slice.FileSlice) error {
	if fq.exitFlag == 1 {
		return errors.New("exiting")
	}
	return fq.putFile2WaitSchQueue(slice)
}

// Put 数据，会序列化到文件
func (fq *fileQueue) PutDatas(datas []models.Data) (err error) {
	fq.RLock()
	defer fq.RUnlock()

	if fq.exitFlag == 1 {
		return errors.New("exiting")
	}
	if fq.currFileSlice == nil || fq.currFileSlice.IsClose() {
		curFileName := fq.fileName(fq.writeFileNum)
		fq.currFileSlice, err = slice.NewFileSlice(fq.writeFileNum, curFileName, fq.writeLimit, 0, 0,
			fq.fileRotateSize, fq.maxMsgSize, fq.format, fq.delim, true)
		if err != nil {
			return err
		}
		fq.sync()

		log.Warnf("FILEQUEUE(%s): writeOne() opened %s", fq.name, curFileName)
	}
	fq.count ++
	err, close := fq.currFileSlice.Write(datas)
	if err != nil {
		log.Warnf("Error: put datas faied. msg:%s", err.Error())
	}
	if close {
		fq.sync()

		fq.writeFileNum++
		fq.putFile2WaitSchQueue(*fq.currFileSlice)
		fq.schedFile2FileSliceChan()
	}

	return
}

// 时间到期时，只有等待上传的队列为空，才强制滚动文件
func (fq *fileQueue) fileRotateIntervalFix() {
	fq.RLock()
	defer fq.RUnlock()
	if len(fq.fileSliceChan)+len(fq.waitSchedQueue) == 0 && fq.currFileSlice.WritePos() > 0 {
		// sync every time we start writing to a new file
		fq.sync()

		// 关闭被滚动文件
		if fq.currFileSlice != nil {
			fq.currFileSlice.Close()
		}

		fq.writeFileNum++
		fq.putFile2WaitSchQueue(*fq.currFileSlice)
		fq.schedFile2FileSliceChan()
	}
}

func (fq *fileQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(fq.dataPath, "%s.%06d"), fq.name, fileNum)
}

func (fq *fileQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(fq.dataPath, "%s.meta"), fq.name)
}

func (fq *fileQueue) queuePersistFileName() string {
	return fmt.Sprintf(path.Join(fq.dataPath, "%s.queue"), fq.name)
}

// sync fsyncs the current writeFile and persists metadata
func (fq *fileQueue) sync() {
	if fq.currFileSlice != nil {
		err := fq.currFileSlice.Sync()
		if err != nil {
			log.Warnf("ERROR: filequeue(%s) failed to sync - %v", fq.name, err)
			return
		}
	}

	err := fq.persistMetaData()
	if err != nil {
		log.Warnf("ERROR: filequeue(%s) failed to sync - %v", fq.name, err)
		return
	}

	fq.needSync = false
	log.Warnf("ERROR: filequeue(%s) failed to sync - %v", fq.name, err)
	return
}

// persistMetaData atomically writes state to the filesystem
func (fq *fileQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := fq.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "%d,%d,%d\n",
		fq.writeFileNum, fq.currFileSlice.WriteCount(), fq.currFileSlice.WritePos())
	if err != nil {
		return err
	}
	f.Sync()
	return os.Rename(tmpFileName, fileName)
}

// 将文件没传成功的文件都进行序列化，方便恢复
func (fq *fileQueue) persistQueueData() error {
	var f *os.File
	var err error

	fileName := fq.queuePersistFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	removeInt := 0
	lineStr := ""
	for _, v := range fq.fileSliceChanMap {
		if v.NeedRemove() {
			removeInt = 1
		} else {
			removeInt = 0
		}
		lineStr = fmt.Sprintf("%s%d,%d,%d,%s\n", lineStr, v.Index(), removeInt, v.WriteCount(), v.FileAbsPath())
	}
	for _, v := range fq.waitSchedQueue {
		if v.NeedRemove() {
			removeInt = 1
		} else {
			removeInt = 0
		}
		lineStr = fmt.Sprintf("%s%d,%d,%d,%s\n", lineStr, v.Index(), removeInt, v.WriteCount(), v.FileAbsPath())
	}
	if _, err = fmt.Fprintln(f, lineStr[:len(lineStr)-1]); err != nil {
		log.Error("persist file queue info fail", err)
		return err
	}
	f.Sync()
	return os.Rename(tmpFileName, fileName)
}

// retrieveMetaData initializes state from the filesystem
func (fq *fileQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := fq.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var writePos int64 = 0
	var writeCount int = 0
	_, err = fmt.Fscanf(f, "%d,%d,%d\n",
		&fq.writeFileNum, &writeCount, &writePos)
	if err != nil {
		return err
	}
	fq.currFileSlice, err = slice.NewFileSlice(fq.writeFileNum, fq.fileName(fq.writeFileNum), fq.writeLimit, writeCount, writePos, fq.fileRotateSize,
		fq.maxMsgSize, fq.format, fq.delim, true)
	if err != nil {
		return err
	}

	qf, err := os.Open(fq.queuePersistFileName())
	if err != nil {
		return err
	}
	defer qf.Close()
	rd := bufio.NewReader(qf)
	slices := []slice.FileSlice{}
	for {
		line, err := rd.ReadString('\n')
		if err != nil || io.EOF == err || len(line) < 5 {
			break
		}
		var index int64
		var ri int
		var count int
		var path string
		_, err = fmt.Sscanf(line, "%d,%d,%d,%s", &index, &ri, &count, &path)
		if err == nil {
			slice, err := slice.NewFinishFileSlice(int64(index), path, count, ri > 0)
			if err != nil {
				log.Warnf("Can not retrieve currFileSlice, index:%d, path:%s, err:%s", index, path, err.Error())
			}
			// 判断当前的fileSlice 是不是已经在队列中，如果在队列中，就将该fileSlice close
			if fq.currFileSlice.Index() == int64(index) {
				fq.currFileSlice.Close()
				fq.writeFileNum ++
			}
			slices = append(slices, *slice)
		} else {
			log.Warnf("Can not retrieve currFileSlice from %s", line)
		}
		fq.waitSchedQueue = slices
	}
	return nil
}

func (fq *fileQueue) Empty() error {
	fq.RLock()
	defer fq.RUnlock()

	if fq.exitFlag == 1 {
		return errors.New("exiting")
	}

	log.Warnf("FILEQUEUE(%s): emptying", fq.name)

	fq.emptyChan <- 1
	return <-fq.emptyResponseChan
}

func (fq *fileQueue) deleteAllFiles() error {
	err := fq.skipToNextRWFile()
	innerErr := os.Remove(fq.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		log.Warnf("ERROR: filequeue(%s) failed to remove metadata file - %s", fq.name, innerErr)
		return innerErr
	}
	innerErr = os.Remove(fq.queuePersistFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		log.Warnf("ERROR: filequeue(%s) failed to remove metadata file - %s", fq.name, innerErr)
		return innerErr
	}
	return err
}

//会删除比writeFileNum小的文件，用户put的file不删除，指删除put的data的数据
func (fq *fileQueue) skipToNextRWFile() error {
	var err error
	fq.currFileSlice.Remove()
	for i := int64(0); i <= fq.writeFileNum; i++ {
		fn := fq.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			log.Warnf("ERROR: filequeue(%s) failed to remove data file - %s", fq.name, innerErr)
			err = innerErr
		}
	}
	fq.writeFileNum++
	return err
}

func (fq *fileQueue) ioLoop() {
	syncTicker := time.NewTicker(fq.syncMetaTimeout)
	fileSliceTicker := time.NewTicker(fq.fileRotateIntervalSecond)
	for {
		if fq.count > fq.syncEveryWrite {
			fq.needSync = true
			fq.count = 0
		}
		if fq.needSync {
			fq.sync()
		}
		select {
		case <-fq.emptyChan:
			fq.emptyResponseChan <- fq.deleteAllFiles()
			fq.count = 0
		case <-syncTicker.C:
			if fq.count > 0 {
				fq.count = 0
				fq.needSync = true
			}
			fq.schedFile2FileSliceChan()
		case <-fileSliceTicker.C:
			fq.fileRotateIntervalFix()
		case <-fq.exitChan:
			goto exit
		}
	}

exit:
	log.Warnf("FILEQUEUE(%s): closing ... ioLoop", fq.name)
	syncTicker.Stop()
	fileSliceTicker.Stop()
	fq.exitSyncChan <- 1
}
