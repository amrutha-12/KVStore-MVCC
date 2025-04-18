package kvstore

// Transaction types and operations
const (
	SUCCESS   = "SUCCESS"
	ABORTED   = "ABORTED"
	COMMITTED = "COMMITTED"
)

type RequestType string

const (
	Put RequestType = "Put"
	Get RequestType = "Get"
)

type Operation struct {
	Type  RequestType
	Key   string
	Value string
}

type Transaction struct {
	ReadSet  map[string]int64
	WriteSet map[string]string
	StartTs  int64
	CommitTs int64
}

type PrepareArgs struct {
	TransactionID string
	ReadSet       map[string]int64
	WriteSet      map[string]string
	StartTs       int64
}

type PrepareReply struct {
	TransactionID   string
	Vote            VoteType
	CurrentVersions map[string]int64
	Err             Err
	CommitTs        int64
}

type VoteType int

const (
	VoteYes VoteType = iota
	VoteNo
	VoteRetry
)

type CommitArgs struct {
	TransactionID string
	CommitTs      int64
	WriteSet      map[string]string
}

type CommitReply struct {
	TransactionID string
	Status        CommitStatus
	Err           Err
}

type CommitStatus int

const (
	Success CommitStatus = iota
	Failed
)

type AbortArgs struct {
	TransactionID string
	AbortTs       int64
}

type AbortReply struct {
	TransactionID string
	Status        AbortStatus
	Err           Err
}

type AbortStatus int

const (
	FullyAborted AbortStatus = iota
	PartialAbort
)
