package kvstore

import (
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Adding support for multiversioning
type Version struct {
	Timestamp int64
	Value     string
}

type Op struct {
	Operation string
	Key       string
	Value     string
	RequestID string
	Timestamp int64
}

type KVStore struct {
	mu   sync.Mutex
	l    net.Listener
	me   int
	dead bool

	data            map[string][]Version
	lamportTime     int64
	visitedRequests map[string]string

	pendingTxns map[string]*TxnState
	locks       map[string]string
}

type TxnState struct {
	ReadSet  map[string]int64
	WriteSet map[string]string
	Status   TxnStatus
	StartTs  int64
}

type TxnStatus int

const (
	Prepared TxnStatus = iota
	Committed
	Aborted
)

func (kv *KVStore) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Operation: "Get",
		Key:       args.Key,
		RequestID: args.RequestID,
		Timestamp: args.Timestamp,
	}

	DPrintf("Server %v received Get: %v\n", kv.me, op)

	ret1, ret2, ts := kv.applyChange(op)
	reply.Err = ret1
	reply.Value = ret2
	reply.Timestamp = ts

	return nil
}

func (kv *KVStore) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Operation: "Put",
		Key:       args.Key,
		Value:     args.Value,
		RequestID: args.RequestID,
		Timestamp: args.Timestamp,
	}

	DPrintf("Server %v received Put: %v\n", kv.me, op)

	ret1, ret2, ts := kv.applyChange(op)
	reply.Err = ret1
	reply.PreviousValue = ret2
	reply.Timestamp = ts

	return nil
}

func (kv *KVStore) applyChange(op Op) (Err, string, int64) {
	kv.lamportTime = max(kv.lamportTime, op.Timestamp)
	versionTimestamp := kv.lamportTime + 1
	kv.lamportTime = versionTimestamp
	if op.Operation == "Put" {

		prevV, found := kv.visitedRequests[op.RequestID]
		if found {
			return OK, prevV, versionTimestamp
		}

		var oldValue string

		if versions := kv.data[op.Key]; len(versions) > 0 {
			// conflict resolution
			lastVersion := versions[len(versions)-1]
			if lastVersion.Timestamp >= versionTimestamp {
				return ErrConflict, "", versionTimestamp
			}
			oldValue = lastVersion.Value
		}

		kv.data[op.Key] = append(kv.data[op.Key], Version{
			Timestamp: versionTimestamp,
			Value:     op.Value,
		})
		kv.visitedRequests[op.RequestID] = oldValue
		fmt.Printf("Server %v: Put(%v) for key(%v) at timestamp(%v)\n", kv.me, op.Value, op.Key, versionTimestamp)
		return OK, oldValue, versionTimestamp

	} else if op.Operation == "Get" {
		versions, found := kv.data[op.Key]
		if !found {
			return ErrNoKey, "", 0
		}

		left, right := 0, len(versions)-1
		for left <= right {
			mid := (left + right) / 2
			if versions[mid].Timestamp <= op.Timestamp {
				left = mid + 1
			} else {
				right = mid - 1
			}
		}

		if right >= 0 {
			// fmt.Printf("Server %v: Get(%v) found version %v\n", kv.me, op.Key, versions[right])
			return OK, versions[right].Value, versions[right].Timestamp
		} else {
			return ErrNoKey, "", 0
		}
	} else {
		return "", "", 0
	}
}

func (kv *KVStore) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
}

// Garbage collection
func (kv *KVStore) garbageCollectVersions(oldestActiveTs int64) {
	for key, versions := range kv.data {
		firstValid := sort.Search(len(versions), func(i int) bool {
			return versions[i].Timestamp >= oldestActiveTs
		})
		kv.data[key] = versions[firstValid:]
	}
}

func (kv *KVStore) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Conflict checking
	for key, readTs := range args.ReadSet {
		if versions := kv.data[key]; len(versions) > 0 {
			if versions[len(versions)-1].Timestamp > readTs {
				reply.Vote = VoteNo
				reply.Err = ErrConflict
				return nil
			}
		}
	}

	for key := range args.WriteSet {
		if owner, exists := kv.locks[key]; exists && owner != args.TransactionID {
			reply.Vote = VoteRetry
			return nil
		}
		kv.locks[key] = args.TransactionID
	}

	kv.pendingTxns[args.TransactionID] = &TxnState{
		ReadSet:  args.ReadSet,
		WriteSet: args.WriteSet,
		Status:   Prepared,
	}

	reply.Vote = VoteYes
	return nil
}

func (kv *KVStore) Commit(args *CommitArgs, reply *CommitReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	txn, exists := kv.pendingTxns[args.TransactionID]
	if !exists || txn.Status != Prepared {
		reply.Status = Failed
		reply.Err = ErrConflict
		return nil
	}
	for key, value := range args.WriteSet {
		kv.data[key] = append(kv.data[key], Version{
			Timestamp: args.CommitTs,
			Value:     value,
		})
		delete(kv.locks, key)
	}
	txn.Status = Committed
	reply.Status = Success
	return nil
}

func (kv *KVStore) Abort(args *AbortArgs, reply *AbortReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	txn, exists := kv.pendingTxns[args.TransactionID]
	if !exists {
		reply.Status = FullyAborted
		return nil
	}

	for key := range txn.WriteSet {
		if kv.locks[key] == args.TransactionID {
			delete(kv.locks, key)
		}
	}

	txn.Status = Aborted
	reply.Status = FullyAborted
	return nil
}

func (kv *KVStore) periodicGC() {
	for !kv.dead {
		time.Sleep(5 * time.Minute)
		kv.mu.Lock()
		oldest := kv.findOldestActiveTxn()
		kv.garbageCollectVersions(oldest)
		kv.mu.Unlock()
	}
}

func (kv *KVStore) findOldestActiveTxn() int64 {
	if len(kv.pendingTxns) == 0 {
		return math.MaxInt64
	}

	oldest := int64(math.MaxInt64)
	for _, txn := range kv.pendingTxns {
		if txn.StartTs < oldest {
			oldest = txn.StartTs
		}
	}
	return oldest
}

func StartServer(servers []string, me int) *KVStore {
	gob.Register(Op{})

	kv := new(KVStore)
	kv.me = me

	kv.data = make(map[string][]Version)
	kv.visitedRequests = make(map[string]string)

	kv.pendingTxns = make(map[string]*TxnState)
	kv.locks = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	sockPath := servers[me]
	os.Remove(sockPath) // Remove existing socket
	l, e := net.Listen("unix", sockPath)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	go func() {
		for !kv.dead {
			conn, err := kv.l.Accept()
			if err == nil && !kv.dead {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && !kv.dead {
				fmt.Printf("KVStore(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go kv.periodicGC()

	return kv
}
