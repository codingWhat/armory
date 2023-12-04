package raft

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"io"
	"os"
)

type Log struct {
	f           *os.File
	reader      *bufio.Reader
	entries     []*logEntry
	startIndex  uint64
	commitIndex uint64
	//sync.RWMutex
	deadlock.RWMutex
	s *Raft
}

func (l *Log) GetEntriesAfter(index uint64) []*logEntry {
	l.RLock()
	defer l.RUnlock()
	if len(l.entries) == 0 || index < 1 {
		return nil
	}

	return l.entries[index-1:]
}

func (l *Log) lastLogInfo() (term uint64, index uint64) {
	l.RLock()
	defer l.RUnlock()
	if len(l.entries) == 0 {
		return 0, l.startIndex + 1
	}
	lastLog := l.entries[len(l.entries)-1]
	return lastLog.Term(), lastLog.Index()
}

func (l *Log) safeAppendEntries(prevLogIdx, prevLogTerm uint64, entries []*logEntry) {
	l.Lock()
	defer l.Unlock()
	// 这种情况只是简单的校验当前节点较leader有不少节点需要被截断，需要leader进行多次prevLogIndex--
	// 性能比较低，这里直接改成循环遍历到正确的index
	//if req.PrevLogIndex > r.startIndex+int64(len(r.log)) {
	//	return newAppendEntryResponse(r.currTerm, r.commitIdx, false).Encode()
	//}
	//清理日志
	//r.truncate(req.PrevLogIndex, req.PrevLogTerm)
	offset := len(l.entries) - 1
	for i := offset; i >= 0; i-- {
		if l.entries[i].Index() == prevLogIdx && l.entries[i].Term() == prevLogTerm {
			offset = i
			break
		}
	}
	//追加日志
	if offset == len(l.entries)-1 {
		l.entries = append(l.entries, entries...)
	} else {
		l.entries = append(l.entries[offset:], entries[offset+1:]...)
	}
}

func (l *Log) getTermByIndex(idx uint64) uint64 {
	l.RLock()
	defer l.RUnlock()
	if len(l.entries) == 0 {
		return 0
	}

	return l.entries[idx-1].Term()
}

func (l *Log) Write(entry *logEntry) error {
	l.Lock()
	defer l.Unlock()
	offset, _ := l.f.Seek(0, os.SEEK_CUR)
	entry.offset = offset

	if len(l.entries) > 0 {
		term, index := l.s.lastLogInfo()
		if entry.Term() < term {
			msg := fmt.Sprintf("new entry's term is outdated, old:%+v, new:%+v", term, entry.Term())
			return errors.New(msg)
		} else if entry.Term() == term && entry.Index() < index {
			msg := fmt.Sprintf("new entry's index is outdated, old:%+v, new:%+v", index, entry.Index())
			return errors.New(msg)
		}
	}

	//err := entry.Encode(l.f)
	//if err != nil {
	//	return err
	//}
	l.entries = append(l.entries, entry)
	return nil
}

func (l *Log) nextIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	if l.startIndex == 0 {
		return uint64(len(l.entries) + 1)
	}
	return l.startIndex + uint64(len(l.entries)+1)
}

func newLog() *Log {
	return &Log{
		entries: make([]*logEntry, 0),
	}
}

func (l *Log) Sync() error {
	return l.f.Sync()
}

func (l *Log) init(path string, s *Raft) error {
	l.s = s
	l.loadCommitIndex()
	return l.open(path)
}

func (l *Log) loadCommitIndex() {
	l.commitIndex = 100
}

func (l *Log) open(path string) error {

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	l.f = f
	l.reader = bufio.NewReader(l.f)
	if stat.Size() == 0 {
		return nil
	}
	//说明有数据,需要加载到状态机中
	var readSize int64 = 0
	for {
		offset, _ := l.f.Seek(0, os.SEEK_CUR)
		entry := newDefaultLogEntry()
		entry.offset = offset
		n, err := entry.Decode(l.reader)
		if err == nil {
			if entry.Index() <= l.startIndex {
				continue
			}
			//正常情况下，追加日志，将小于等于commitIndex的数据应用到状态机中
			//commitIndex 会从独立的文件先加载
			//startIndex是 如果有快照数据就是最后一个entry的Index, 没有的话就是0
			l.entries = append(l.entries, entry)
			if entry.Index() <= l.commitIndex {
				err = entry.cmd.Apply(l.s)
				if err != nil {
					return err
				}
			}

			readSize += int64(n)
		} else {
			if errors.Is(err, io.EOF) {
				break
			}
			err := l.f.Truncate(readSize)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type logEntry struct {
	diskLogEntry *diskLogEntry
	term         uint64
	index        uint64
	offset       int64

	cmd Command
	c   chan error
}

func newLogEntry(term uint64, index uint64, cmd Command) *logEntry {
	b, err := json.Marshal(cmd)
	if err != nil {
		fmt.Println("---->newLogEntry, Marshal err:", err.Error())
	}
	return &logEntry{
		diskLogEntry: &diskLogEntry{Index: index, Term: term, CommandName: cmd.Name(), Command: b},
		term:         term,
		index:        index,
		cmd:          cmd,
	}
}

func newDefaultLogEntry() *logEntry {
	return &logEntry{
		diskLogEntry: &diskLogEntry{},
	}
}

func (l *logEntry) Index() uint64 {
	return l.index
}

func (l *logEntry) Term() uint64 {
	return l.term
}

type diskLogEntry struct {
	Index       uint64 `json:"Index,omitempty"`
	Term        uint64 `json:"Term,omitempty"`
	CommandName string `json:"CommandName,omitempty"`
	Command     []byte `json:"Command,omitempty"`
}

func (d *diskLogEntry) encode() ([]byte, error) {
	return json.Marshal(d)
}

func (l *logEntry) Encode(w io.Writer) error {

	b, _ := l.diskLogEntry.encode()
	_, err := fmt.Fprintf(w, "%8x\n", len(b))
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func (l *logEntry) makeCommand(name string, val []byte) Command {
	cmd, ok := GetCommand(name)
	if !ok {
		panic("command: " + name + " not register")
	}
	_ = json.Unmarshal(val, cmd)
	return cmd
}

func (l *logEntry) Decode(r io.Reader) (int, error) {
	var length int
	_, err := fmt.Fscanf(r, "%8x\n", &length)
	if err != nil {
		return -1, err
	}

	data := make([]byte, length)
	_, err = io.ReadFull(r, data)

	if err != nil {
		return -1, err
	}

	err = json.Unmarshal(data, l.diskLogEntry)
	if err != nil {
		return -1, err
	}

	l.index = l.diskLogEntry.Index
	l.term = l.diskLogEntry.Term
	l.cmd = l.makeCommand(l.diskLogEntry.CommandName, l.diskLogEntry.Command)
	//return length + 8 + 1, nil
	return length + 8, nil
}
