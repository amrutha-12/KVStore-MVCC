package kvstore

const (
	OK          = "OK"
	ErrNoKey    = "ErrNoKey"
	ErrConflict = "ErrConflict"
)

type Err string

type PutArgs struct {
	Key       string
	Value     string
	RequestID string
	Timestamp int64
}

type PutReply struct {
	Err           Err
	PreviousValue string
	Timestamp     int64
}

type GetArgs struct {
	Key       string
	RequestID string
	Timestamp int64
}

type GetReply struct {
	Err       Err
	Value     string
	Timestamp int64
}
