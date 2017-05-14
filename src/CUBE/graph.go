package CUBE

import (
	"sync"
	"sync/atomic"
)

func (cube *CUBE) TargetNode(vertex int) int {
	return vertex % (cube.N / cube.L)
}

func (cube *CUBE) AddVertex(vertex int, data *VertexData) {
	atomic.AddUint64(&cube.NPureVertices, 1)
	targetNode := cube.TargetNode(vertex)
	for i := 0; i < cube.L; i++ {
		cube.SendCommand(targetNode + i * cube.NperL,
			CmdAddVertex{i: vertex, data: cube.SplitVertexData(data, i)})
	}
}
func (cube *CUBE) AddEdge(s int, t int, data *EdgeData) {
	atomic.AddUint64(&cube.NPureEdges, 1)
	targetNode := cube.TargetNode(t)
	for i := 0; i < cube.L; i++ {
		cube.SendCommand(targetNode + i * cube.NperL,
			CmdAddEdge{s: s, t: t, data: cube.SplitEdgeData(data, i)})
	}
	if cube.TargetNode(s) != cube.TargetNode(t) {
		for i := 0; i < cube.L; i++ {
			cube.SendCommand(cube.TargetNode(s) + i * cube.NperL,
				CmdAddEdgeMirror{s: s, t: t})
		}
	}
}
func (cube *CUBE) FinalizeGraph(threshold int) {
	var wg sync.WaitGroup
	wg.Add(cube.N)
	for i := 0; i < cube.N; i++ {
		go func(i int) {
			resp := make(chan interface{})
			cube.SendCommand(i, CmdFinalizeGraph{threshold: threshold, resp: resp})
			<-resp
			wg.Done()
		}(i)
	}
	wg.Wait()
}