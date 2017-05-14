package CUBE

func (worker *Worker) cmdSink(cmd CmdSink) {
	for _, e := range worker.edges {
		if e.mirror {
			continue
		}
		var u, v *VertexData
		if worker.vertices[e.s].master != worker.node {
			resp := make(chan *VertexData)
			worker.cube.SendInternal(worker.layer_base + worker.vertices[e.s].master,
				CmdFetchVertex{i: worker.vertices[e.s].localId, resp: resp})
			u = <-resp
		} else {
			u = worker.vertices[e.s].data
		}
		v = worker.vertices[e.t].data
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
	if worker.cube.sync {
		cmd.done <- nil
	}
}
