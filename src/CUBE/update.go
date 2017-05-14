package CUBE

func (worker *Worker) cmdUpdateEdge(cmd CmdUpdateEdge) {
	for eid, e := range worker.edges {
		if e.mirror {
			continue
		}
		data := make([]*EdgeData, worker.cube.L)
		data[0] = worker.edges[eid].data
		for i := 1; i < worker.cube.L; i++ {
			resp := make(chan *EdgeData)
			worker.cube.SendInternal(worker.node + i * worker.cube.NperL, CmdFetchEdge{eid: eid, resp: resp})
			tmp := <-resp
			data[i] = tmp
		}
		combinedData := worker.cube.CombineEdgeData(data)
		cmd.f(combinedData)
		worker.edges[eid].data = worker.cube.SplitEdgeData(combinedData, 0)
		for i := 1; i < worker.cube.L; i++ {
			worker.cube.SendInternal(worker.node + i * worker.cube.NperL,
				CmdPushEdge{eid: eid, data: worker.cube.SplitEdgeData(combinedData, i)})
		}
	}
	if worker.cube.sync {
		cmd.done <- nil
	}
}