package CUBE

import "sync"

func (cube *CUBE) TargetNode(vertex int) int {
	return vertex % (cube.N / cube.L)
}

func (cube *CUBE) AddVertex(vertex int, data *VertexData) {
	targetNode := cube.TargetNode(vertex)
	for i := 0; i < cube.L; i++ {
		cube.SendCommand(targetNode + i * cube.N / cube.L,
			CmdAddVertex{i: vertex, data: cube.SplitVertexData(data, i)})
	}
}
func (cube *CUBE) AddEdge(s int, t int, data *EdgeData) {
	targetNode := cube.TargetNode(t)
	for i := 0; i < cube.L; i++ {
		cube.SendCommand(targetNode + i * cube.N / cube.L,
			CmdAddEdge{s: s, t: t, data: cube.SplitEdgeData(data, i)})
	}
	if cube.TargetNode(s) != cube.TargetNode(t) {
		for i := 0; i < cube.L; i++ {
			cube.SendCommand(cube.TargetNode(s) + i * cube.N / cube.L,
				CmdAddEdgeMirror{s: s, t: t})
		}
	}
}
func (cube *CUBE) CountIngress(i int) int {
	resp := make(chan int)
	cube.SendCommand(cube.TargetNode(i), CmdCountIngress{i: i, resp: resp})
	ret := <-resp
	return ret
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