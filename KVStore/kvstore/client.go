package kvstore

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"strconv"
	"sync/atomic"
	"time"
)

type Client struct {
	servers       []string
	transactionId int64
	clientID      int64
	lamportTime   int64
}

func MakeClient(servers []string) *Client {
	ck := new(Client)
	ck.servers = servers
	ck.transactionId = 1
	ck.clientID = nrand()
	return ck
}

func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	return err == nil
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigX, _ := rand.Int(rand.Reader, max)
	x := bigX.Int64()
	return x
}

var globalId int64 = 0

func (ck *Client) generateID() string {
	var ret string
	ck.transactionId++
	atomic.AddInt64(&globalId, 1)
	ret = strconv.FormatInt(ck.clientID, 10) + "-" +
		time.Now().String() + "-" +
		strconv.FormatInt(ck.transactionId, 10) + "-" +
		strconv.FormatInt(nrand(), 10) + "-" +
		strconv.FormatInt(globalId, 10)
	return ret
}

func (ck *Client) BeginTxn() *Transaction {
	startTs := atomic.AddInt64(&ck.lamportTime, 1)
	return &Transaction{
		ReadSet:  make(map[string]int64),
		WriteSet: make(map[string]string),
		StartTs:  startTs,
	}
}

func (ck *Client) Get(key string, timestamp int64) (string, Err, int64) {
	if timestamp == 0 {
		timestamp = atomic.AddInt64(&ck.lamportTime, 1)
	}
	args := GetArgs{
		Key:       key,
		RequestID: ck.generateID(),
		Timestamp: timestamp,
	}
	var reply GetReply
	for {
		for _, server := range ck.servers {
			ok := call(server, "KVStore.Get", &args, &reply)
			if ok {
				ck.updateLamportTime(reply.Timestamp)
				if reply.Err == OK {
					return reply.Value, reply.Err, reply.Timestamp
				}
				return "", reply.Err, reply.Timestamp
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Client) Put(key string, value string, timestamp int64) (Err, string, int64) {
	timestamp = atomic.AddInt64(&ck.lamportTime, 1)
	args := PutArgs{
		Key:       key,
		Value:     value,
		RequestID: ck.generateID(),
		Timestamp: timestamp,
	}
	// fmt.Printf("Client %v sending Put: %v\n", ck.clientID, args)
	var reply PutReply
	for {
		for _, server := range ck.servers {
			ok := call(server, "KVStore.Put", &args, &reply)
			if ok {
				ck.updateLamportTime(reply.Timestamp)
				// fmt.Printf("Client %v received Put reply: %v\n", ck.clientID, reply)
				if reply.Err == OK {
					return reply.Err, reply.PreviousValue, reply.Timestamp
				}
				if reply.Err == ErrConflict {
					args.Timestamp = atomic.AddInt64(&ck.lamportTime, 1)
					continue
				}
				return reply.Err, reply.PreviousValue, reply.Timestamp
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Client) updateLamportTime(serverTs int64) {
	for {
		current := atomic.LoadInt64(&ck.lamportTime)
		newTimestamp := max(current, serverTs) + 1
		if atomic.CompareAndSwapInt64(&ck.lamportTime, current, newTimestamp) {
			break
		}
	}
}

// Transaction handling
func (ck *Client) CommitTxn(txn *Transaction) error {

	txnID := ck.generateID()
	prepareArgs := PrepareArgs{
		TransactionID: txnID,
		ReadSet:       txn.ReadSet,
		WriteSet:      txn.WriteSet,
		StartTs:       txn.StartTs,
	}

	var prepareReplies []PrepareReply
	for _, server := range ck.servers {
		var reply PrepareReply
		ok := call(server, "KVStore.Prepare", &prepareArgs, &reply)
		if !ok || reply.Vote != VoteYes {
			ck.abortTxn(txn)
			return fmt.Errorf("prepare phase failed")
		}
		prepareReplies = append(prepareReplies, reply)
	}

	// Phase 2: Commit
	if len(prepareReplies) < len(ck.servers) {
		ck.abortTxn(txn)
		return fmt.Errorf("partial prepare")
	}
	commitTs := atomic.AddInt64(&ck.lamportTime, 1)
	commitArgs := CommitArgs{
		TransactionID: prepareArgs.TransactionID,
		CommitTs:      commitTs,
		WriteSet:      txn.WriteSet,
	}

	for _, server := range ck.servers {
		var reply CommitReply
		ok := call(server, "KVStore.Commit", &commitArgs, &reply)
		if !ok || reply.Status != Success {
			ck.abortTxn(txn)
			return fmt.Errorf("commit phase failed")
		}
	}

	// Apply writes locally
	// for key, value := range txn.WriteSet {
	// 	ck.Put(key, value, commitTs)
	// }

	return nil
}

func (ck *Client) abortTxn(txn *Transaction) {
	abortArgs := AbortArgs{
		TransactionID: ck.generateID(),
		AbortTs:       txn.StartTs,
	}

	for _, server := range ck.servers {
		var reply AbortReply
		call(server, "KVStore.Abort", &abortArgs, &reply)
	}
}
