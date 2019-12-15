package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"encoding/gob"
	"bytes"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType	string
	Args	interface{}
}

type Result struct {
	opType	string
	args  interface{}
	reply interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	uid 		int64
	cfg			shardmaster.Config
	mck			*shardmaster.Clerk

	kvdb 		[shardmaster.NShards]map[string]string
	lastseq 	map[int64]int	
	replych 	map[int]chan Result
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{OpType: Get, Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	if _, ok := kv.replych[index]; !ok {
		kv.replych[index] = make(chan Result, 1)

	}
	chanMsg := kv.replych[index]
	kv.mu.Unlock()
	select {
	case msg := <- chanMsg:
		if recArgs, ok := msg.args.(GetArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recArgs.ClientId || args.RequestId != recArgs.RequestId {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(GetReply)
				reply.WrongLeader = false
			}
		}
	case <- time.After(200 * time.Millisecond):
		reply.WrongLeader = true
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{OpType: PutAppend, Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	if _, ok := kv.replych[index]; !ok {
		kv.replych[index] = make(chan Result, 1)

	}
	chanMsg := kv.replych[index]
	kv.mu.Unlock()
	select {
	case msg := <- chanMsg:
		if tmpArgs, ok := msg.args.(PutAppendArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != tmpArgs.ClientId || args.RequestId != tmpArgs.RequestId {
				reply.WrongLeader = true
			} else {
				reply.Err = msg.reply.(PutAppendReply).Err
				reply.WrongLeader = false
			}
		}
	case <- time.After(200 * time.Millisecond):
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendReply{})
	gob.Register(GetReply{})
	gob.Register(shardmaster.Config{})
	gob.Register(ReconfigureArgs{})
	gob.Register(ReconfigureReply{})
	gob.Register(TransferArgs{})
	gob.Register(TransferReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.uid = nrand()
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.replych = make(map[int]chan Result)
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastseq = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.kvdb[i] = make(map[string]string)
	}
	kv.LoadSnapShot(persister.ReadSnapshot())

	go kv.UpdateOp(persister)
	go kv.PollConfig()

	return kv
}


func (kv *ShardKV) SendTransferShard(gid int, args *TransferArgs, reply *TransferReply) bool {
	for _, server := range kv.cfg.Groups[gid] {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.TransferShard", args, reply)
		if ok {
			if reply.Err == OK {
				return true
			} else if reply.Err == ErrNotReady {
				return false
			}
		}
	}
	return false
}

func (kv *ShardKV) TransferShard(args *TransferArgs, reply *TransferReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.cfg.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}
	reply.Err = OK
	reply.Ack = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		reply.StoreShard[i] = make(map[string]string)
	}
	for _, shardIndex := range args.Shards {
		for k, v := range kv.kvdb[shardIndex] {
			reply.StoreShard[shardIndex][k] = v
		}
	}

	for clientId := range kv.lastseq {
		reply.Ack[clientId] = kv.lastseq[clientId]
	}
}

func (kv *ShardKV) GetReconfigure(nextCfg shardmaster.Config) (ReconfigureArgs, bool) {
	retArgs := ReconfigureArgs{Cfg:nextCfg}
	retArgs.Ack = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		retArgs.StoreShard[i] = make(map[string]string)
	}
	retOk := true
	transShards := make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.cfg.Shards[i] != kv.gid && nextCfg.Shards[i] == kv.gid {
			gid := kv.cfg.Shards[i]
			if gid != 0 {
				if _, ok := transShards[gid]; !ok {
					transShards[gid] = []int{i}
				} else {
					transShards[gid] = append(transShards[gid], i)
				}
			}
		}
	}
	var lastseqMutex sync.Mutex
	var wait sync.WaitGroup
	for gid, value := range transShards {
		wait.Add(1)
		go func(gid int, value []int) {
			defer wait.Done()
			var reply TransferReply
			if kv.SendTransferShard(gid, &TransferArgs{ConfigNum:nextCfg.Num, Shards:value}, &reply) {
				lastseqMutex.Lock()
				for shardIndex, data := range reply.StoreShard {
					for k, v := range data {
						retArgs.StoreShard[shardIndex][k] = v
					}
				}
				for clientId := range reply.Ack {
					if _, exist := retArgs.Ack[clientId]; !exist || retArgs.Ack[clientId] < reply.Ack[clientId] {
						retArgs.Ack[clientId] = reply.Ack[clientId]
					}
				}
				lastseqMutex.Unlock()
			} else {
				retOk = false
			}
		} (gid, value)
	}
	wait.Wait()
	return retArgs, retOk
}

func (kv *ShardKV) SyncReconfigure(args ReconfigureArgs) bool {
	for i := 0; i < 3; i++ {
		index, _, isLeader := kv.rf.Start(Op{OpType:Reconfigure, Args:args})
		if !isLeader {
			return false
		}
		kv.mu.Lock()
		if _, ok := kv.replych[index]; !ok {
			kv.replych[index] = make(chan Result, 1)

		}
		chanMsg := kv.replych[index]
		kv.mu.Unlock()

		select {
		case msg := <- chanMsg:
			if tmpArgs, ok := msg.args.(ReconfigureArgs); ok {
				if args.Cfg.Num == tmpArgs.Cfg.Num {
					return true
				}
			}
		case <- time.After(200 * time.Millisecond):
			continue
		}
	}
	return false
}

func (kv *ShardKV) PollConfig() {
	for true {
		if _, isLeader := kv.rf.GetState(); isLeader {
			latestCfg := kv.mck.Query(-1)
			for i := kv.cfg.Num + 1; i <= latestCfg.Num; i++ {
				args, ok := kv.GetReconfigure(kv.mck.Query(i))
				if !ok {
					break
				}
				if !kv.SyncReconfigure(args) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) UpdateOp(persister *raft.Persister) {
	for true {
		msg := <- kv.applyCh
		if msg.UseSnapshot {
			if msg.Command != nil {
				op := msg.Command.(Op)
				kv.ApplyOp(op)// apply unsnapshot log data
			}else {
				kv.LoadSnapShot(persister.ReadSnapshot())// read snapshot
			}
		} else {
			var result Result
			request := msg.Command.(Op)
			result.args = request.Args
			result.opType = request.OpType
			result.reply = kv.ApplyOp(request)
			kv.SendResult(msg.Index, result)
			kv.IfSnapshot(msg.Index)
		}
	}
}

func (kv *ShardKV) LoadSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var arr1 []int
	kv.lastseq = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.kvdb[i] = make(map[string]string)
	}
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	d.Decode(&arr1)
	d.Decode(&kv.cfg)
	d.Decode(&kv.kvdb)
	d.Decode(&kv.lastseq)
}

func (kv * ShardKV) IfSnapshot(index int) {
	if kv.maxraftstate != -1 && float64(kv.rf.GetLogSize()) > float64(kv.maxraftstate)*0.8 {
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(kv.cfg)
		e.Encode(kv.kvdb)
		e.Encode(kv.lastseq)
		data := w.Bytes()
		go kv.rf.StartSnapshot(data, index)
	}
}

func (kv *ShardKV) SendResult(index int, result Result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.replych[index]; !ok {
		kv.replych[index] = make(chan Result, 1)
	} else {
		select {
		case <- kv.replych[index]:
		default:
		}
	}
	kv.replych[index] <- result
}

func (kv *ShardKV) ApplyOp(request Op) interface{} {
	switch request.Args.(type) {
	case GetArgs:
		return kv.ApplyGet(request.Args.(GetArgs))
	case PutAppendArgs:
		return kv.ApplyPutAppend(request.Args.(PutAppendArgs))
	case ReconfigureArgs:
		return kv.ApplyReconfigure(request.Args.(ReconfigureArgs))
	}
	return nil
}

func (kv *ShardKV) ApplyGet(args GetArgs) GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var reply GetReply
	if !kv.CheckValidKey(args.Key) {
		reply.Err = ErrWrongGroup
		return reply
	}
	if value, ok := kv.kvdb[key2shard(args.Key)][args.Key]; ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
	return reply
}

func (kv *ShardKV) ApplyPutAppend(args PutAppendArgs) PutAppendReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var reply PutAppendReply
	if !kv.CheckValidKey(args.Key) {
		reply.Err = ErrWrongGroup
		return reply
	}
	if !kv.CheckSeqNum(args.ClientId, args.RequestId) {
		if args.Op == Put {
			kv.kvdb[key2shard(args.Key)][args.Key] = args.Value
		} else {
			kv.kvdb[key2shard(args.Key)][args.Key] += args.Value
		}
	}
	reply.Err = OK
	return reply
}

func (kv *ShardKV) ApplyReconfigure(args ReconfigureArgs) ReconfigureReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var reply ReconfigureReply
	if args.Cfg.Num > kv.cfg.Num {
		for shardIndex, data := range args.StoreShard {
			for k, v := range data {
				kv.kvdb[shardIndex][k] = v
			}
		}
		for clientId := range args.Ack {
			if _, exist := kv.lastseq[clientId]; !exist || kv.lastseq[clientId] < args.Ack[clientId] {
				kv.lastseq[clientId] = args.Ack[clientId]
			}
		}
		kv.cfg = args.Cfg
		reply.Err = OK
	}
	return reply
}

func (kv *ShardKV) CheckSeqNum(clientId int64, requestId int) bool {
	if value, ok := kv.lastseq[clientId]; ok && value >= requestId {
		return true
	}
	kv.lastseq[clientId] = requestId
	return false
}

func (kv *ShardKV) CheckValidKey(key string) bool {
	shardId := key2shard(key)
	if kv.gid != kv.cfg.Shards[shardId] {
		return false
	}
	return true
}