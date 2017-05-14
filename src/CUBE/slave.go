package CUBE

import (
	"fmt"
	"sync"
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
	node                  int
	layer_base            int
	localSc               int
	vertices              []Vertex
	edges                 []Edge
	finalizing, finalized bool
	finalize_sync         sync.WaitGroup
}

func (worker *Worker) handleInternal(internal chan interface{}) {
	for _cmd := range internal {
		switch _cmd.(type) {
		case CmdCountIngress:
			cmd := _cmd.(CmdCountIngress)
			cmd.resp <- CountIngress(cmd.i, worker.edges)
			break
		case CmdAddVertexMirror:
			cmd := _cmd.(CmdAddVertexMirror)
			found := false
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
			break
		case CmdAddEdgeMirror:
			if worker.finalizing {
				panic("fuck")
			}
			cmd := _cmd.(CmdAddEdgeMirror)
			worker.edges = append(worker.edges, Edge{s: cmd.s, t: cmd.t, mirror: true})
			break
		case CmdGetLocalId:
			cmd := _cmd.(CmdGetLocalId)
			found := false
			for vid, v := range worker.vertices {
				if v.i == cmd.i {
					cmd.resp <- vid
					found = true
					break
				}
			}
			if !found {
				panic("Vertex not found")
			}
			break
		case CmdFetchVertex:
			if !worker.finalized {
				panic("fuck")
			}
			cmd := _cmd.(CmdFetchVertex)
			cmd.resp <- worker.vertices[cmd.i].data
			break
		case CmdFetchEdge:
			if !worker.finalized {
				panic("fuck")
			}
			cmd := _cmd.(CmdFetchEdge)
			cmd.resp <- worker.edges[cmd.eid].data
			break
		case CmdPushEdge:
			if !worker.finalized {
				panic("fuck")
			}
			cmd := _cmd.(CmdPushEdge)
			worker.edges[cmd.eid].data = cmd.data
			break
		case CmdMirrorVertexPull:
			cmd := _cmd.(CmdMirrorVertexPull)
			sum := make([]interface{}, worker.localSc)
			for _, e := range worker.edges {
				if !e.mirror && worker.vertices[e.s].i == cmd.globalId {
					v := *worker.vertices[e.t].data
					for i, colled := range v.Colle {
						sum[i] = cmd.sum(
							sum[i],
							cmd.g(v.Share, colled, e.data.Share, e.data.Colle[i]))
					}
				}
			}
			cmd.resp <- &sum
			break
		case CmdFinalizeSync:
			worker.finalize_sync.Done()
		case CmdMirrorVertexPush:
			cmd := _cmd.(CmdMirrorVertexPush)
			sum := make([]interface{}, worker.localSc)
			for _, e := range worker.edges {
				if !e.mirror && e.t == cmd.localId {
					v := worker.vertices[e.t].data
					for i, colled := range v.Colle {
						sum[i] = cmd.sum(
							sum[i],
							cmd.g(v.Share, colled, e.data.Share, e.data.Colle[i]))
					}
				}
			}
			cmd.resp <- &sum
		}
	}
}

func (worker *Worker) handleCmd(cmds chan interface{}) {
	for _cmd := range cmds {
		switch _cmd.(type) {
		case CmdAddEdge:
			if worker.finalizing {
				panic("fuck")
			}
			cmd := _cmd.(CmdAddEdge)
			worker.edges = append(worker.edges, Edge{s: cmd.s, t: cmd.t, data: cmd.data})
			break
		case CmdAddVertex:
			if worker.finalizing {
				panic("fuck")
			}
			cmd := _cmd.(CmdAddVertex)
			worker.vertices = append(worker.vertices,
				Vertex{i: cmd.i, data: cmd.data, master: worker.node, localId: len(worker.vertices)})
			break
		case CmdFinalizeGraph:
			if worker.finalizing {
				panic("fuck")
			}
			worker.finalizing = true
			cmd := _cmd.(CmdFinalizeGraph)

			// First, collect all vertices' in-degree
			// And distribute high-degree vertices
			for vid, v := range worker.vertices {
				if v.master == worker.node && CountIngress(vid, worker.edges) > cmd.threshold {
					for i := 0; i < worker.cube.N / worker.cube.L; i++ {
						if worker.layer_base + i == worker.node {
							continue
						}
						var movedEdge []Edge = []Edge{}
						for j, e := range worker.edges {
							if e.t == v.i && worker.cube.TargetNode(e.s) != worker.node % worker.cube.NperL {
								movedEdge = append(movedEdge, e)
								if j == len(worker.edges) - 1 {
									worker.edges = worker.edges[:j]
								} else {
									worker.edges = append(worker.edges[:j], worker.edges[j + 1:]...)
								}
							}
						}
						resp := make(chan int)
						worker.cube.SendInternal(worker.layer_base + i,
							CmdAddVertexMirror{i: v.i, data: v.data, localId: vid, edges: movedEdge, resp: resp})
						v.mirrors = append(v.mirrors, worker.layer_base + i, <-resp)
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
			for i, v := range worker.vertices {
				vexist[v.i] = i
			}

			// Create mirror vertices if needed
			for i, e := range worker.edges {
				snode := worker.cube.TargetNode(e.s)
				if snode != worker.node {
					if _, ok := vexist[e.s]; !ok {
						worker.vertices = append(worker.vertices, Vertex{i: e.s, master: snode})
						vexist[e.s] = len(worker.vertices) - 1
						resp := make(chan int)
						worker.cube.SendInternal(worker.layer_base + worker.cube.TargetNode(e.s),
							CmdGetLocalId{i: e.s, resp: resp})
						worker.vertices[len(worker.vertices) - 1].localId = <-resp
					}
				}
				tnode := worker.cube.TargetNode(e.t)
				if tnode != worker.node {
					if _, ok := vexist[e.t]; !ok {
						worker.vertices = append(worker.vertices, Vertex{i: e.t, master: tnode})
						vexist[e.t] = len(worker.vertices) - 1
						resp := make(chan int)
						worker.cube.SendInternal(worker.layer_base + worker.cube.TargetNode(e.t),
							CmdGetLocalId{i: e.t, resp: resp})
						worker.vertices[len(worker.vertices) - 1].localId = <-resp
					}
				}
				worker.edges[i].s = vexist[e.s]
				worker.edges[i].t = vexist[e.t]
			}

			// Finalization finish
			worker.finalized = true
			println("Node", worker.node, "has", len(worker.vertices), "vertices,", len(worker.edges), "edges")
			cmd.resp <- nil
		case CmdSink:
			if !worker.finalized {
				panic("fuck")
			}
			worker.cmdSink(_cmd.(CmdSink))
		case CmdUpdateEdge:
			if !worker.finalized {
				panic("fuck")
			}
			worker.cmdUpdateEdge(_cmd.(CmdUpdateEdge))
		case CmdDump:
			if !worker.finalized {
				panic("fuck")
			}
			cmd := _cmd.(CmdDump)
			for _, v := range worker.vertices {
				if v.master == worker.node {
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
				data := make([]*EdgeData, worker.cube.L)
				data[0] = e.data
				for i := 1; i < worker.cube.L; i++ {
					resp := make(chan *EdgeData)
					worker.cube.SendInternal(worker.node + i * worker.cube.NperL, CmdFetchEdge{eid: eid, resp: resp})
					tmp := <-resp
					data[i] = tmp
				}
				combinedData := worker.cube.CombineEdgeData(data)
				for _, v := range combinedData.Colle {
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
				panic("fuck")
			}
			worker.cmdPush(_cmd.(CmdPush))
		case CmdPull:
			if !worker.finalized {
				panic("fuck")
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
	worker.localSc = cube.LowerBound(node / cube.NperL + 1) - cube.LowerBound(node / cube.NperL)
	worker.vertices = make([]Vertex, 0)
	worker.edges = make([]Edge, 0)
	worker.finalizing, worker.finalized = false, false
	worker.finalize_sync.Add(cube.NperL - 1)
	go worker.handleInternal(internal)
	go worker.handleCmd(cmds)
}
