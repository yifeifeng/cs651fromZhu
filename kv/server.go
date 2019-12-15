package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string
	ClientId int64
	SeqNum   int
}


type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdb		map[string]string
	lastseq 	map[int64]int
	replych	    map[int]chan Op
}

func (kv *RaftKV) SendToRaft(op1 Op) bool {
	index, _, isLeader := kv.rf.Start(op1)
	if !isLeader {
		return false
	}
	kv.mu.Lock()
	ch, ok := kv.replych[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.replych[index] = ch
	}
	kv.mu.Unlock()
	//fmt.Printf("kvlog:%v\n", op1)
	select {
		case op := <-ch: //receive from reply channel
			if op == op1 {
				return true
			}else {
				return false
			}
		case <-time.After(1200*time.Millisecond)://timeout
			//fmt.Printf("cc\n")
			return false
	}
}

func (kv *RaftKV) CheckSeqNum(id int64,seqnum int) bool { //check duplicate request
	lseq, ok := kv.lastseq[id]
	if ok {
		if lseq >= seqnum {
			return true
		}else {
			return false
		}
	}
	return false
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op
	op.OpType = "Get"
	op.Key = args.Key
	op.ClientId = args.ClientId
	op.SeqNum = args.SeqNum
	ok := kv.SendToRaft(op)
	if ok {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.kvdb[args.Key]
		kv.lastseq[args.ClientId] = args.SeqNum
		kv.mu.Unlock()
	}else {
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op
	op.OpType = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.ClientId = args.ClientId
	op.SeqNum = args.SeqNum
	ok := kv.SendToRaft(op)
	if ok {
		reply.WrongLeader = false
		reply.Err = OK
	}else {
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) Apply(args Op) { //apply data to kv storage
	if !kv.CheckSeqNum(args.ClientId, args.SeqNum) {
		if args.OpType == "Put" {
			kv.kvdb[args.Key] = args.Value
		}else {
			kv.kvdb[args.Key] += args.Value
		}
		kv.lastseq[args.ClientId] = args.SeqNum
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) LoadSnapshot(snapshot []byte){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var arr1 []int
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	d.Decode(&arr1)
	d.Decode(&kv.kvdb)
	d.Decode(&kv.lastseq)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvdb = make(map[string]string)
	kv.lastseq = make(map[int64]int)
	kv.replych = make(map[int]chan Op)
	kv.LoadSnapshot(persister.ReadSnapshot())
	go func() {
		for {
			applymsg := <-kv.applyCh
			if applymsg.UseSnapshot {
				//fmt.Printf("%v\n", applymsg.Command)
				if applymsg.Command != nil {// recover from failure or finish snapshot
					kv.mu.Lock()
					op := applymsg.Command.(Op)
					kv.Apply(op)// apply unsnapshot log data
					kv.mu.Unlock()
				}else {
					kv.LoadSnapshot(persister.ReadSnapshot())// read snapshot
				}
			}else {
				op := applymsg.Command.(Op)
				Index := applymsg.Index
				//fmt.Printf("op:%v msgindex:%v\n", op, Index)
				kv.mu.Lock()
				kv.Apply(op)
				ch, ok := kv.replych[Index]
				if ok {
					select {
						case <-ch:
						default:
					}
					ch<-op
				} else {
					kv.replych[Index] = make(chan Op, 1)
				}
				if maxraftstate != -1 && kv.rf.GetLogSize() > maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.kvdb)
					e.Encode(kv.lastseq)
					data := w.Bytes()
					go kv.rf.StartSnapshot(data,applymsg.Index)
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}