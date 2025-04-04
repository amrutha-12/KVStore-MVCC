package kvstore

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Operation string // can be GET or PUT
	Key       string
	Value     string
	RequestID string
}

type KVStore struct {
	mu   sync.Mutex
	l    net.Listener
	me   int
	dead bool

	data            map[string]string
	visitedRequests map[string]string
}

func (kv *KVStore) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Operation: "Get",
		Key:       args.Key,
		RequestID: args.RequestID,
	}

	DPrintf("Server %v received Get: %v\n", kv.me, op)

	ret1, ret2 := kv.applyChange(op)
	reply.Err = ret1
	reply.Value = ret2

	return nil
}

func (kv *KVStore) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Operation: "Put",
		Key:       args.Key,
		Value:     args.Value,
		RequestID: args.RequestID,
	}

	DPrintf("Server %v received Put: %v\n", kv.me, op)

	ret1, ret2 := kv.applyChange(op)
	reply.Err = ret1
	reply.PreviousValue = ret2

	return nil
}

// applyChange Applies the operation (Op) to the server database.
// the caller should hold the lock
func (kv *KVStore) applyChange(op Op) (Err, string) {
	if op.Operation == "Put" {

		prevV, found := kv.visitedRequests[op.RequestID]
		if found {
			return OK, prevV
		}

		oldValue := kv.data[op.Key]

		newValue := op.Value

		DPrintf("Server %v, apply op: %v, old: %v, new: %v\n", kv.me, op, oldValue, newValue)

		kv.data[op.Key] = newValue

		kv.visitedRequests[op.RequestID] = oldValue
		return OK, oldValue

	} else if op.Operation == "Get" {
		value, found := kv.data[op.Key]

		if found {
			return OK, value
		} else {
			return ErrNoKey, ""
		}
	} else {
		return "", ""
	}
}

func (kv *KVStore) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
}

func StartServer(servers []string, me int) *KVStore {
	gob.Register(Op{})

	kv := new(KVStore)
	kv.me = me

	kv.data = make(map[string]string)
	kv.visitedRequests = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVStore(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
