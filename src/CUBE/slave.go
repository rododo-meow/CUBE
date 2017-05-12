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
func slave(cube *CUBE, node int, cmds chan interface{}, internal chan interface{}) {
	layer_base := node / cube.NperL * cube.NperL
	localSc := cube.LowerBound(node / cube.NperL + 1) - cube.LowerBound(node / cube.NperL)
	vertices := make([]Vertex, 0)
	edges := make([]Edge, 0)
	finalizing, finalized := false, false
	var finalize_sync sync.WaitGroup
	finalize_sync.Add(cube.NperL - 1)
	go func() {
		for _cmd := range internal {
			switch _cmd.(type) {
			case CmdCountIngress:
				cmd := _cmd.(CmdCountIngress)
				cmd.resp <- CountIngress(cmd.i, edges)
				break
			case CmdAddVertexMirror:
				cmd := _cmd.(CmdAddVertexMirror)
				found := false
				for vid, v := range vertices {
					if v.i == cmd.i {
						v.mirror = true
						v.master = cube.TargetNode(v.i)
						v.data = cmd.data
						v.localId = cmd.localId
						cmd.resp <- vid
						found = true
						break
					}
				}
				if !found {
					vertices = append(vertices,
						Vertex{i: cmd.i, mirror: true, master: cube.TargetNode(cmd.i), data: cmd.data, localId: cmd.localId})
					cmd.resp <- len(vertices) - 1
				}
				break
			case CmdAddEdgeMirror:
				if finalizing {
					panic("fuck")
				}
				cmd := _cmd.(CmdAddEdgeMirror)
				edges = append(edges, Edge{s: cmd.s, t: cmd.t, mirror: true})
				break
			case CmdGetLocalId:
				cmd := _cmd.(CmdGetLocalId)
				found := false
				for vid, v := range vertices {
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
				if !finalized {
					panic("fuck")
				}
				cmd := _cmd.(CmdFetchVertex)
				cmd.resp <- vertices[cmd.i].data
				break
			case CmdFetchEdge:
				if !finalized {
					panic("fuck")
				}
				cmd := _cmd.(CmdFetchEdge)
				cmd.resp <- edges[cmd.eid].data
				break
			case CmdPushEdge:
				if !finalized {
					panic("fuck")
				}
				cmd := _cmd.(CmdPushEdge)
				edges[cmd.eid].data = cmd.data
				break
			case CmdMirrorVertexPull:
				cmd := _cmd.(CmdMirrorVertexPull)
				sum := make([]interface{}, localSc)
				for _, e := range edges {
					if !e.mirror && vertices[e.s].i == cmd.globalId {
						v := *vertices[e.t].data
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
				finalize_sync.Done()
			case CmdMirrorVertexPush:
				cmd := _cmd.(CmdMirrorVertexPush)
				sum := make([]interface{}, localSc)
				for _, e := range edges {
					if !e.mirror && e.t == cmd.localId {
						v := vertices[e.t].data
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
	}()
	for _cmd := range cmds {
		switch _cmd.(type) {
		case CmdAddEdge:
			if finalizing {
				panic("fuck")
			}
			cmd := _cmd.(CmdAddEdge)
			edges = append(edges, Edge{s: cmd.s, t: cmd.t, data: cmd.data})
			break
		case CmdAddVertex:
			if finalizing {
				panic("fuck")
			}
			cmd := _cmd.(CmdAddVertex)
			vertices = append(vertices, Vertex{i: cmd.i, data: cmd.data, master: node, localId: len(vertices)})
			break
		case CmdFinalizeGraph:
			if finalizing {
				panic("fuck")
			}
			finalizing = true
			cmd := _cmd.(CmdFinalizeGraph)

			// First, collect all vertices' in-degree
			// And distribute high-degree vertices
			for vid, v := range vertices {
				if v.master == node && CountIngress(vid, edges) > cmd.threshold {
					for i := 0; i < cube.N / cube.L; i++ {
						if layer_base + i == node {
							continue
						}
						var movedEdge []Edge = []Edge{}
						for j, e := range edges {
							if e.t == v.i && cube.TargetNode(e.s) != node % cube.NperL {
								movedEdge = append(movedEdge, e)
								if j == len(edges) - 1 {
									edges = edges[:j]
								} else {
									edges = append(edges[:j], edges[j + 1:]...)
								}
							}
						}
						resp := make(chan int)
						cube.SendInternal(layer_base + i,
							CmdAddVertexMirror{i: v.i, data: v.data, localId: vid, edges: movedEdge, resp: resp})
						v.mirrors = append(v.mirrors, layer_base + i, <-resp)
					}
				}
			}

			// Then, send synchronization signal
			for i := 0; i < cube.NperL; i++ {
				if layer_base + i == node {
					continue
				}
				cube.SendInternal(layer_base + i, CmdFinalizeSync{})
			}

			// Wait for all other nodes finish distribution
			finalize_sync.Wait()

			// Generate globalId->localId mapping
			vexist := make(map[int]int)
			for i, v := range vertices {
				vexist[v.i] = i
			}

			// Create mirror vertices if needed
			for i, e := range edges {
				snode := cube.TargetNode(e.s)
				if snode != node {
					if _, ok := vexist[e.s]; !ok {
						vertices = append(vertices, Vertex{i: e.s, master: snode})
						vexist[e.s] = len(vertices) - 1
						resp := make(chan int)
						cube.SendInternal(layer_base + cube.TargetNode(e.s),
							CmdGetLocalId{i: e.s, resp: resp})
						vertices[len(vertices) - 1].localId = <-resp
					}
				}
				tnode := cube.TargetNode(e.t)
				if tnode != node {
					if _, ok := vexist[e.t]; !ok {
						vertices = append(vertices, Vertex{i: e.t, master: tnode})
						vexist[e.t] = len(vertices) - 1
						resp := make(chan int)
						cube.SendInternal(layer_base + cube.TargetNode(e.t),
							CmdGetLocalId{i: e.t, resp: resp})
						vertices[len(vertices) - 1].localId = <-resp
					}
				}
				edges[i].s = vexist[e.s]
				edges[i].t = vexist[e.t]
			}

			// Finalization finish
			finalized = true
			println("Node", node, "has", len(vertices), "vertices,", len(edges), "edges")
			cmd.resp <- nil
			break
		case CmdSink:
			if !finalized {
				panic("fuck")
			}
			cmd := _cmd.(CmdSink)
			for _, e := range edges {
				if e.mirror {
					continue
				}
				var u, v *VertexData
				if vertices[e.s].master != node {
					resp := make(chan *VertexData)
					cube.SendInternal(layer_base + vertices[e.s].master,
						CmdFetchVertex{i: vertices[e.s].localId, resp: resp})
					u = <-resp
				} else {
					u = vertices[e.s].data
				}
				v = vertices[e.t].data
				for i := range e.data.Colle {
					e.data.Colle[i] = cmd.f(
						u.Share,
						u.Colle[i],
						v.Share,
						v.Colle[i],
						e.data.Share,
						e.data.Colle[i])
				}
			}
			if cube.sync {
				cmd.done <- nil
			}
			break
		case CmdUpdateEdge:
			if !finalized {
				panic("fuck")
			}
			cmd := _cmd.(CmdUpdateEdge)
			for eid, e := range edges {
				if e.mirror {
					continue
				}
				data := make([]*EdgeData, cube.L)
				data[0] = edges[eid].data
				for i := 1; i < cube.L; i++ {
					resp := make(chan *EdgeData)
					cube.SendInternal(node + i * cube.NperL, CmdFetchEdge{eid: eid, resp: resp})
					tmp := <-resp
					data[i] = tmp
				}
				combinedData := cube.CombineEdgeData(data)
				cmd.f(combinedData)
				edges[eid].data = cube.SplitEdgeData(combinedData, 0)
				for i := 1; i < cube.L; i++ {
					cube.SendInternal(node + i * cube.NperL,
						CmdPushEdge{eid: eid, data: cube.SplitEdgeData(combinedData, i)})
				}
			}
			if cube.sync {
				cmd.done <- nil
			}
			break
		case CmdDump:
			if !finalized {
				panic("fuck")
			}
			cmd := _cmd.(CmdDump)
			for _, v := range vertices {
				if v.master == node {
					str := fmt.Sprint("Vertex[", v.i, "]: ")
					if v.data.Share != nil {
						str += "share " + v.data.Share.Dump() + " "
					}
					str += "colle ["
					data := make([]*VertexData, cube.L)
					data[0] = v.data
					for i := 1; i < cube.L; i++ {
						resp := make(chan *VertexData)
						cube.SendInternal(node + i * cube.NperL, CmdFetchVertex{i: v.localId, resp: resp})
						tmp := <-resp
						data[i] = tmp
					}
					combinedData := cube.CombineVertexData(data)
					for _, v := range combinedData.Colle {
						str += v.Dump() + ", "
					}
					str += "]"
					println(str)
				}
			}
			for eid, e := range edges {
				if e.mirror {
					continue
				}
				str := fmt.Sprint("Edge[", vertices[e.s].i, "->", vertices[e.t].i, "]: ")
				if e.data.Share != nil {
					str += "share " + e.data.Share.Dump() + " "
				}
				str += "colle ["
				data := make([]*EdgeData, cube.L)
				data[0] = e.data
				for i := 1; i < cube.L; i++ {
					resp := make(chan *EdgeData)
					cube.SendInternal(node + i * cube.NperL, CmdFetchEdge{eid: eid, resp: resp})
					tmp := <-resp
					data[i] = tmp
				}
				combinedData := cube.CombineEdgeData(data)
				for _, v := range combinedData.Colle {
					str += v.Dump() + ", "
				}
				str += "]"
				println(str)
			}
			if cube.sync {
				cmd.done <- nil
			}
			break
		case CmdPush:
			if !finalized {
				panic("fuck")
			}
			cmd := _cmd.(CmdPush)
			for vid, v := range vertices {
				if v.master == node {
					sum := make([]interface{}, cube.LowerBound(node / cube.NperL + 1) - cube.LowerBound(node / cube.NperL))
					if len(v.mirrors) != 0 {
						// High-degree vertex, need to collect more data from mirror vertices
						for i := 0; i < len(v.mirrors) / 2; i++ {
							resp := make(chan *[]interface{})
							cube.SendInternal(v.mirrors[i * 2],
								CmdMirrorVertexPush{localId: v.mirrors[i * 2 + 1], resp: resp})
							tmp := <-resp
							for i, s := range *tmp {
								sum[i] = cmd.sum(sum[i], s)
							}
						}
					}
					for _, e := range edges {
						if e.t == vid {
							var u *VertexData
							if vertices[e.s].master == node || vertices[e.s].mirror {
								// Local vertex
								uvertex := vertices[e.s]
								u = uvertex.data
							} else {
								// Pull data
								resp := make(chan *VertexData)
								cube.SendInternal(layer_base + vertices[e.s].master,
									CmdFetchVertex{i: vertices[e.s].localId, resp: resp})
								u = <-resp
							}
							for i, colled := range u.Colle {
								sum[i] = cmd.sum(
									sum[i],
									cmd.g(
										u.Share,
										colled,
										e.data.Share,
										e.data.Colle[i]))
							}
						}
					}
					for i, d := range sum {
						v.data.Colle[i] = cmd.a(v.data.Share, v.data.Colle[i], d)
					}
				}
			}
			if cube.sync {
				cmd.done <- nil
			}
			break
		case CmdPull:
			if !finalized {
				panic("fuck")
			}
			cmd := _cmd.(CmdPull)
			for vid, u := range vertices {
				if u.master == node {
					sum := make([]interface{}, cube.LowerBound(node / cube.NperL + 1) - cube.LowerBound(node / cube.NperL))
					collected := make([]bool, cube.N / cube.L)
					for i := range collected {
						collected[i] = false
					}
					for _, e := range edges {
						if e.s == vid {
							var v VertexData
							if e.mirror {
								if !collected[vertices[e.t].master] {
									// Pull data
									resp := make(chan *[]interface{})
									cube.SendInternal(layer_base + vertices[e.t].master,
										CmdMirrorVertexPull{globalId: u.i, resp: resp, g: cmd.g, sum: cmd.sum})
									tmp := <-resp
									for i := range *tmp {
										sum[i] = cmd.sum(sum[i], (*tmp)[i])
									}
								}
							} else {
								// Local edge
								v = *vertices[e.t].data
								for i, colled := range v.Colle {
									sum[i] = cmd.sum(
										sum[i],
										cmd.g(v.Share, colled, e.data.Share, e.data.Colle[i]))
								}
							}
						}
					}
					for i, d := range sum {
						u.data.Colle[i] = cmd.a(u.data.Share, u.data.Colle[i], d)
					}
				}
			}
			if cube.sync {
				cmd.done <- nil
			}
			break
		}
	}
	cube.wg.Done()
}
