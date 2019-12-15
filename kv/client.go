package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	seqNum   int
	leaderId int
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seqNum = 0
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	var args GetArgs
	args.Key = key
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.SeqNum = ck.seqNum
	ck.seqNum++
	ck.mu.Unlock()
	i := ck.leaderId
	for {
		var reply GetReply
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.mu.Lock()
			ck.leaderId = i
			ck.mu.Unlock()
			if reply.Err == OK {
				return reply.Value
			}else {
				return ""
			}
		}
		i = (i+1)%len(ck.servers)// retry until find the leader
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.SeqNum = ck.seqNum
	ck.seqNum++
	ck.mu.Unlock()
	i := ck.leaderId
	for {
		var reply PutAppendReply
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.mu.Lock()
			ck.leaderId = i
			ck.mu.Unlock()
			return
		}
		i = (i+1)%len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}