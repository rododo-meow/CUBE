package CUBE

import (
	"sync/atomic"
)

type CmdAddEdge struct {
	s, t int
	data *EdgeData
}

type CmdAddVertex struct {
	i    int
	data *VertexData
}

type CmdFinalizeGraph struct {
	threshold int
	resp      chan interface{}
}

type CmdAddVertexMirror struct {
	i, localId int
	data       *VertexData
	edges      []Edge
	resp       chan int
}

type CmdSink struct {
	f    SinkFunc
	done chan interface{}
}

type CmdFetchVertex struct {
	i    int
	resp chan *VertexData
}

type CmdPushVertex struct {
	localId int
	data    *VertexData
}

type CmdGetLocalId struct {
	i    int
	resp chan int
}

type CmdUpdateEdge struct {
	f             UpdateEdgeFunc
	localCombiner LocalCombinerFunc
	done          chan interface{}
}

type CmdFetchEdge struct {
	eid           int
	resp          chan []Data
	localCombiner LocalCombinerFunc
}

type CmdPushEdge struct {
	eid  int
	data *EdgeData
}

type CmdPushEdgeShare struct {
	eid  int
	data Data
}

type CmdDump struct {
	done chan interface{}
}

type CmdPush struct {
	g    GatherFunc
	a    ApplyFunc
	sum  SumFunc
	done chan interface{}
}

type CmdPull struct {
	g    GatherFunc
	a    ApplyFunc
	sum  SumFunc
	done chan interface{}
}

type CmdAddEdgeMirror struct {
	s, t int
}

type CmdMirrorVertexPull struct {
	globalId int
	resp     chan *[]interface{}
	g        GatherFunc
	sum      SumFunc
}

type CmdFinalizeSync struct {
}

type CmdMirrorVertexPush struct {
	localId int
	resp    chan *[]interface{}
	g       GatherFunc
	sum     SumFunc
}

type RPCCounter struct {
	Internal     uint64
	InternalSize uint64
}

func (cube *CUBE) SendCommand(i int, cmd interface{}) {
	cube.chans[i] <- cmd
}

func (cube *CUBE) SendInternal(i int, cmd interface{}) {
	switch cmd.(type) {
	case CmdFetchVertex:
		atomic.AddUint64(&cube.RpcCount.Internal, 1)
		atomic.AddUint64(
			&cube.RpcCount.InternalSize,
			uint64(1 + cube.LowerBound(i % cube.NperL + 1) - cube.LowerBound(i % cube.NperL)))
	case CmdFetchEdge:
		atomic.AddUint64(&cube.RpcCount.Internal, 1)
		if cmd.(CmdFetchEdge).localCombiner != nil {
			atomic.AddUint64(
				&cube.RpcCount.InternalSize,
				1)
		} else {
			atomic.AddUint64(
				&cube.RpcCount.InternalSize,
				uint64(cube.LowerBound(i % cube.NperL + 1) - cube.LowerBound(i % cube.NperL)))
		}
	case CmdPushEdge:
		atomic.AddUint64(&cube.RpcCount.Internal, 1)
		atomic.AddUint64(
			&cube.RpcCount.InternalSize,
			uint64(1 + cube.LowerBound(i % cube.NperL + 1) - cube.LowerBound(i % cube.NperL)))
	case CmdPushVertex:
		atomic.AddUint64(&cube.RpcCount.Internal, 1)
		atomic.AddUint64(
			&cube.RpcCount.InternalSize,
			uint64(1 + cube.LowerBound(i % cube.NperL + 1) - cube.LowerBound(i % cube.NperL)))
	case CmdPushEdgeShare:
		atomic.AddUint64(&cube.RpcCount.Internal, 1)
		atomic.AddUint64(
			&cube.RpcCount.InternalSize,
			1)
	case CmdMirrorVertexPull:
		atomic.AddUint64(&cube.RpcCount.Internal, 1)
		atomic.AddUint64(
			&cube.RpcCount.InternalSize,
			uint64(cube.LowerBound(i % cube.NperL + 1) - cube.LowerBound(i % cube.NperL)))
	case CmdMirrorVertexPush:
		atomic.AddUint64(&cube.RpcCount.Internal, 1)
		atomic.AddUint64(
			&cube.RpcCount.InternalSize,
			uint64(cube.LowerBound(i % cube.NperL + 1) - cube.LowerBound(i % cube.NperL)))
	}
	cube.internal_chans[i] <- cmd
}
