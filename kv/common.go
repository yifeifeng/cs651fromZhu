package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break
	ClientId int64
	SeqNum   int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	LeaderId    int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqNum   int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
