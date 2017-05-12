package CUBE

import (
	"fmt"
	"sync"
)

type CUBE struct {
	N, L, Sc, NperL int
	chans           []chan interface{}
	internal_chans  []chan interface{}
	wg              sync.WaitGroup
	sync            bool
	RpcCount        RPCCounter
}

type GatherFunc func(vshared Data, vcolle Data, eshared Data, ecolle Data) interface{}
type ApplyFunc func(vshared Data, vcolle Data, sum interface{}) Data
type SumFunc func(a interface{}, b interface{}) interface{}
type SinkFunc func(uShare Data, uColle Data, vShare Data, vColle Data, eShare Data, eColle Data) Data

func CreateCUBE(N int, L int, Sc int, synchronous bool) (*CUBE, error) {
	if N == 0 || L == 0 {
		return nil, fmt.Errorf("N and L shouldn't be 0")
	}
	if N % L != 0 {
		return nil, fmt.Errorf("N % L should be 0")
	}
	cube := &CUBE{
		N: N,
		L: L,
		NperL: N / L,
		sync: synchronous,
		chans: make([]chan interface{}, N),
		internal_chans: make([]chan interface{}, N),
		Sc: Sc,
		RpcCount:RPCCounter{
			Internal: 0,
			InternalSize: 0,
		},
	}
	cube.wg.Add(N)
	for i := 0; i < N; i++ {
		cube.chans[i] = make(chan interface{}, 10)
		cube.internal_chans[i] = make(chan interface{}, 10)
		go slave(cube, i, cube.chans[i], cube.internal_chans[i])
	}
	return cube, nil
}

func (cube *CUBE) Sink(f SinkFunc) {
	if cube.sync {
		for i := 0; i < cube.N; i++ {
			done := make(chan interface{})
			cube.SendCommand(i, CmdSink{f: f, done: done})
			<-done
		}
	} else {
		for i := 0; i < cube.N; i++ {
			cube.SendCommand(i, CmdSink{f: f})
		}
	}
}

func (cube *CUBE) UpdateEdge(f func(*EdgeData)) {
	if cube.sync {
		for i := 0; i < cube.N / cube.L; i++ {
			done := make(chan interface{})
			cube.SendCommand(i, CmdUpdateEdge{f: f, done: done})
			<-done
		}
	} else {
		for i := 0; i < cube.N / cube.L; i++ {
			cube.SendCommand(i, CmdUpdateEdge{f: f})
		}
	}
}

func (cube *CUBE) Push(g GatherFunc, a ApplyFunc, sum SumFunc) {
	if cube.sync {
		for i := 0; i < cube.N; i++ {
			done := make(chan interface{})
			cube.SendCommand(i, CmdPush{g: g, a: a, sum: sum, done: done})
			<-done
		}
	} else {
		for i := 0; i < cube.N; i++ {
			cube.SendCommand(i, CmdPush{g: g, a: a, sum: sum})
		}
	}
}

func (cube *CUBE) Pull(g GatherFunc, a ApplyFunc, sum SumFunc) {
	if cube.sync {
		for i := 0; i < cube.N; i++ {
			done := make(chan interface{})
			cube.SendCommand(i, CmdPull{g: g, a: a, sum: sum, done: done})
			<-done
		}
	} else {
		for i := 0; i < cube.N; i++ {
			cube.SendCommand(i, CmdPull{g: g, a: a, sum: sum})
		}
	}
}

func (cube *CUBE) Dump() {
	if cube.sync {
		for i := 0; i < cube.N / cube.L; i++ {
			done := make(chan interface{})
			cube.SendCommand(i, CmdDump{done: done})
			<-done
		}
	} else {
		for i := 0; i < cube.N / cube.L; i++ {
			cube.SendCommand(i, CmdDump{})
		}
	}
}

func (cube *CUBE) Wait() {
	for _, ch := range cube.chans {
		close(ch)
	}
	cube.wg.Wait()
}