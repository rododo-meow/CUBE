package CUBE

import "sync"

func (worker *Worker) cmdUpdateEdge(cmd CmdUpdateEdge) {
	for eid, e := range worker.edges {
		if e.mirror {
			continue
		}
		var wg sync.WaitGroup
		wg.Add(worker.cube.L - 1)
		var colle []Data
		if cmd.localCombiner != nil {
			colle = make([]Data, worker.cube.L)
			colle[0] = cmd.localCombiner(e.data.Share, e.data.Colle)
			for i := 1; i < worker.cube.L; i++ {
				go func(i int) {
					defer wg.Done()
					resp := make(chan []Data)
					worker.cube.SendInternal(worker.node + i * worker.cube.NperL,
						CmdFetchEdge{eid: eid, resp: resp, localCombiner: cmd.localCombiner})
					tmp := <-resp
					colle[i] = tmp[0]
				}(i)
			}
		} else {
			colle = make([]Data, worker.cube.Sc)
			copy(colle, e.data.Colle)
			for i := 1; i < worker.cube.L; i++ {
				go func(i int) {
					defer wg.Done()
					resp := make(chan []Data)
					worker.cube.SendInternal(worker.node + i * worker.cube.NperL,
						CmdFetchEdge{eid: eid, resp: resp, localCombiner: nil})
					tmp := <-resp
					copy(colle[worker.cube.LowerBound(i):], tmp)
				}(i)
			}
		}
		wg.Wait()
		newedge := cmd.f(e.data.Share, colle)
		if cmd.localCombiner != nil {
			worker.edges[eid].data.Share = newedge.Share
			for i := 1; i < worker.cube.L; i++ {
				worker.cube.SendInternal(worker.node + i * worker.cube.NperL,
					CmdPushEdgeShare{eid: eid, data: newedge.Share})
			}
		} else {
			worker.edges[eid].data = worker.cube.SplitEdgeData(newedge, 0)
			for i := 1; i < worker.cube.L; i++ {
				worker.cube.SendInternal(worker.node + i * worker.cube.NperL,
					CmdPushEdge{eid: eid, data: newedge})
			}
		}
	}
	if worker.cube.sync {
		cmd.done <- nil
	}
}