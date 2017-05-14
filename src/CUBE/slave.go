package CUBE

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Vertex struct {
	i       int
	data    *VertexData
	master  int
	mirror  bool
	localId int
	mirrors []int
}
type Edge struct {
	s, t   int
	data   *EdgeData
	mirror bool
}

func CountIngress(vertex int, edges []Edge) int {
	ret := 0
	for _, e := range edges {
		if e.t == vertex {
			ret++
		}
	}
	return ret
}

type Worker struct {
	cube                  *CUBE
	node, nodeInLayer     int
	layer_base            int
	localSc               int
	vertices              []Vertex
	edges                 []Edge
	finalizing, finalized bool
	finalize_sync         sync.WaitGroup
	elock                 sync.RWMutex
	vlock                 sync.RWMutex
}

func (worker *Worker) handleInternal(internal chan interface{}) {
	for _cmd := range internal {
		switch _cmd.(type) {
		case CmdAddVertexMirror:
			cmd := _cmd.(CmdAddVertexMirror)
			found := false
			worker.elock.Lock()
			worker.edges = append(worker.edges, cmd.edges...)
			worker.elock.Unlock()
			worker.vlock.Lock()
			for vid, v := range worker.vertices {
				if v.i == cmd.i {
					v.mirror = true
					v.master = worker.cube.TargetNode(v.i)
					v.data = cmd.data
					v.localId = cmd.localId
					cmd.resp <- vid
					found = true
					break
				}
			}
			if !found {
				worker.vertices = append(worker.vertices,
					Vertex{i: cmd.i, mirror: true, master: worker.cube.TargetNode(cmd.i), data: cmd.data, localId: cmd.localId})
				cmd.resp <- len(worker.vertices) - 1
			}
			worker.vlock.Unlock()
			break
		case CmdAddEdgeMirror:
			if worker.finalizing {
				panic("Call FinalizeGraph() first")
			}
			cmd := _cmd.(CmdAddEdgeMirror)
			worker.elock.Lock()
			worker.edges = append(worker.edges, Edge{s: cmd.s, t: cmd.t, mirror: true})
			worker.elock.Unlock()
			break
		case CmdGetLocalId:
			cmd := _cmd.(CmdGetLocalId)
			found := false
			worker.vlock.RLock()
			for vid, v := range worker.vertices {
				if v.i == cmd.i {
					cmd.resp <- vid
					found = true
					break
				}
			}
			worker.vlock.RUnlock()
			if !found {
				panic("Vertex not found")
			}
			break
		case CmdFetchVertex:
			if !worker.finalized {
				panic("Call FinalizeGraph() first")
			}
			cmd := _cmd.(CmdFetchVertex)
			worker.vlock.RLock()
			cmd.resp <- worker.vertices[cmd.i].data
			worker.vlock.RUnlock()
			break
		case CmdFetchEdge:
			if !worker.finalized {
				panic("Call FinalizeGraph() first")
			}
			cmd := _cmd.(CmdFetchEdge)
			worker.elock.RLock()
			if cmd.localCombiner != nil {
				cmd.resp <- []Data{
					cmd.localCombiner(worker.edges[cmd.eid].data.Share, worker.edges[cmd.eid].data.Colle),
				}
			} else {
				cmd.resp <- worker.edges[cmd.eid].data.Colle
			}
			worker.elock.RUnlock()
			break
		case CmdPushEdge:
			if !worker.finalized {
				panic("Call FinalizeGraph() first")
			}
			cmd := _cmd.(CmdPushEdge)
			worker.elock.Lock()
			worker.edges[cmd.eid].data = cmd.data
			worker.elock.Unlock()
		case CmdPushEdgeShare:
			if !worker.finalized {
				panic("Call FinalizeGraph() first")
			}
			cmd := _cmd.(CmdPushEdgeShare)
			worker.elock.Lock()
			worker.edges[cmd.eid].data.Share = cmd.data
			worker.elock.Unlock()
		case CmdMirrorVertexPull:
			worker.cmdMirrorPull(_cmd.(CmdMirrorVertexPull))
		case CmdFinalizeSync:
			worker.finalize_sync.Done()
		case CmdMirrorVertexPush:
			worker.cmdMirrorPush(_cmd.(CmdMirrorVertexPush))
		case CmdPushVertex:
			cmd := _cmd.(CmdPushVertex)
			worker.vlock.Lock()
			worker.vertices[cmd.localId].data = cmd.data
			worker.vlock.Unlock()
		}
	}
}

func (worker *Worker) handleCmd(cmds chan interface{}) {
	for _cmd := range cmds {
		switch _cmd.(type) {
		case CmdAddEdge:
			if worker.finalizing {
				panic("Never add edge after FinalizeGraph()")
			}
			cmd := _cmd.(CmdAddEdge)
			worker.elock.Lock()
			worker.edges = append(worker.edges, Edge{s: cmd.s, t: cmd.t, data: cmd.data})
			worker.elock.Unlock()
			break
		case CmdAddVertex:
			if worker.finalizing {
				panic("Never add vertex after FinalizeGraph()")
			}
			cmd := _cmd.(CmdAddVertex)
			worker.vlock.Lock()
			worker.vertices = append(worker.vertices,
				Vertex{i: cmd.i, data: cmd.data, master: worker.nodeInLayer, localId: len(worker.vertices)})
			worker.vlock.Unlock()
			break
		case CmdFinalizeGraph:
			if worker.finalizing {
				panic("Never FinalizeGraph() twice")
			}
			worker.finalizing = true
			cmd := _cmd.(CmdFinalizeGraph)

			// First, collect all vertices' in-degree
			// And distribute high-degree vertices
			for vid := 0; ;vid++ {
				worker.vlock.RLock()
				if vid >= len(worker.vertices) {
					worker.vlock.RUnlock()
					break
				}
				v := worker.vertices[vid]
				worker.vlock.RUnlock()
				if v.master == worker.nodeInLayer && CountIngress(v.i, worker.edges) > cmd.threshold {
					for i := 0; i < worker.cube.NperL; i++ {
						if worker.layer_base + i == worker.node {
							continue
						}
						var movedEdge []Edge = []Edge{}
						worker.elock.Lock()
						for j := 0; j < len(worker.edges); j++ {
							e := worker.edges[j]
							if e.t == v.i && worker.cube.TargetNode(e.s) == i {
								movedEdge = append(movedEdge, e)
								worker.edges[j] = worker.edges[len(worker.edges) - 1]
								worker.edges = worker.edges[:len(worker.edges) - 1]
								j--
							}
						}
						worker.elock.Unlock()
						resp := make(chan int)
						worker.cube.SendInternal(worker.layer_base + i,
							CmdAddVertexMirror{i: v.i, data: v.data, localId: vid, edges: movedEdge, resp: resp})
						worker.vertices[vid].mirrors = append(v.mirrors, worker.layer_base + i, <-resp)
					}
				}
			}

			// Then, send synchronization signal
			for i := 0; i < worker.cube.NperL; i++ {
				if worker.layer_base + i == worker.node {
					continue
				}
				worker.cube.SendInternal(worker.layer_base + i, CmdFinalizeSync{})
			}

			// Wait for all other nodes finish distribution
			worker.finalize_sync.Wait()

			// Generate globalId->localId mapping
			vexist := make(map[int]int)
			worker.vlock.RLock()
			for i, v := range worker.vertices {
				vexist[v.i] = i
			}
			worker.vlock.RUnlock()

			// Create mirror vertices if needed
			worker.elock.Lock()
			for i, e := range worker.edges {
				if _, ok := vexist[e.s]; !ok {
					worker.vertices = append(worker.vertices, Vertex{i: e.s, master: worker.cube.TargetNode(e.s)})
					vexist[e.s] = len(worker.vertices) - 1
					resp := make(chan int)
					worker.cube.SendInternal(worker.layer_base + worker.cube.TargetNode(e.s),
						CmdGetLocalId{i: e.s, resp: resp})
					worker.vertices[len(worker.vertices) - 1].localId = <-resp
				}
				if _, ok := vexist[e.t]; !ok {
					worker.vertices = append(worker.vertices, Vertex{i: e.t, master: worker.cube.TargetNode(e.t)})
					vexist[e.t] = len(worker.vertices) - 1
					resp := make(chan int)
					worker.cube.SendInternal(worker.layer_base + worker.cube.TargetNode(e.t),
						CmdGetLocalId{i: e.t, resp: resp})
					worker.vertices[len(worker.vertices) - 1].localId = <-resp
				}
				worker.edges[i].s = vexist[e.s]
				worker.edges[i].t = vexist[e.t]
			}
			worker.elock.Unlock()

			// Finalization finish
			worker.finalized = true
			atomic.AddUint64(&worker.cube.NVertices, uint64(len(worker.vertices)))
			cmd.resp <- nil
		case CmdSink:
			if !worker.finalized {
				panic("Call FinalizeGraph() first")
			}
			worker.cmdSink(_cmd.(CmdSink))
		case CmdUpdateEdge:
			if !worker.finalized {
				panic("Call FinalizeGraph() first")
			}
			worker.cmdUpdateEdge(_cmd.(CmdUpdateEdge))
		case CmdDump:
			if !worker.finalized {
				panic("Call FinalizeGraph() first")
			}
			cmd := _cmd.(CmdDump)
			for _, v := range worker.vertices {
				if v.master == worker.nodeInLayer {
					str := fmt.Sprint("Vertex[", v.i, "]: ")
					if v.data.Share != nil {
						str += "share " + v.data.Share.Dump() + " "
					}
					str += "colle ["
					data := make([]*VertexData, worker.cube.L)
					data[0] = v.data
					for i := 1; i < worker.cube.L; i++ {
						resp := make(chan *VertexData)
						worker.cube.SendInternal(worker.node + i * worker.cube.NperL, CmdFetchVertex{i: v.localId, resp: resp})
						tmp := <-resp
						data[i] = tmp
					}
					combinedData := worker.cube.CombineVertexData(data)
					for _, v := range combinedData.Colle {
						str += v.Dump() + ", "
					}
					str += "]"
					println(str)
				}
			}
			for eid, e := range worker.edges {
				if e.mirror {
					continue
				}
				str := fmt.Sprint("Edge[", worker.vertices[e.s].i, "->", worker.vertices[e.t].i, "]: ")
				if e.data.Share != nil {
					str += "share " + e.data.Share.Dump() + " "
				}
				str += "colle ["
				data := make([]Data, worker.cube.Sc)
				copy(data, e.data.Colle)
				for i := 1; i < worker.cube.L; i++ {
					resp := make(chan []Data)
					worker.cube.SendInternal(worker.node + i * worker.cube.NperL, CmdFetchEdge{eid: eid, resp: resp})
					tmp := <-resp
					copy(data[worker.cube.LowerBound(i):], tmp)
				}
				for _, v := range data {
					str += v.Dump() + ", "
				}
				str += "]"
				println(str)
			}
			if worker.cube.sync {
				cmd.done <- nil
			}
		case CmdPush:
			if !worker.finalized {
				panic("Call FinalizeGraph() first")
			}
			worker.cmdPush(_cmd.(CmdPush))
		case CmdPull:
			if !worker.finalized {
				panic("Call FinalizeGraph() first")
			}
			worker.cmdPull(_cmd.(CmdPull))
		}
	}
	worker.cube.wg.Done()
}

func slave(cube *CUBE, node int, cmds chan interface{}, internal chan interface{}) {
	worker := new(Worker)
	worker.cube = cube
	worker.node = node
	worker.layer_base = node / cube.NperL * cube.NperL
	worker.nodeInLayer = worker.node - worker.layer_base
	worker.localSc = cube.LowerBound(node / cube.NperL + 1) - cube.LowerBound(node / cube.NperL)
	worker.vertices = make([]Vertex, 0)
	worker.edges = make([]Edge, 0)
	worker.finalizing, worker.finalized = false, false
	worker.finalize_sync.Add(cube.NperL - 1)
	go worker.handleInternal(internal)
	go worker.handleCmd(cmds)
}
