package kvstore

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutArgs struct {
	Key       string
	Value     string
	RequestID string
}

type PutReply struct {
	Err           Err
	PreviousValue string
}

type GetArgs struct {
	Key       string
	RequestID string
}

type GetReply struct {
	Err   Err
	Value string
}
