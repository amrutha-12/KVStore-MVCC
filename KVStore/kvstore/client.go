package kvstore

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"strconv"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers       []string
	transactionId int64
	clientID      int64
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
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
	if err == nil {
		return true
	}

	return false
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigX, _ := rand.Int(rand.Reader, max)
	x := bigX.Int64()
	return x
}

var globalId int64 = 0

func (ck *Clerk) generateID() string {
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

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		RequestID: ck.generateID(),
	}
	var reply GetReply
	for {
		for _, server := range ck.servers {
			ok := call(server, "KVStore.Get", &args, &reply)
			if ok {
				return reply.Value
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) string {
	args := PutArgs{
		Key:       key,
		Value:     value,
		RequestID: ck.generateID(),
	}
	var reply PutReply
	for {
		for _, server := range ck.servers {
			ok := call(server, "KVStore.Put", &args, &reply)
			if ok {
				return reply.PreviousValue
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}
