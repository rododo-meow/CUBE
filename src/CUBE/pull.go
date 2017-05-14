package CUBE

func (worker *Worker) cmdPull(cmd CmdPull) {
	for vid, u := range worker.vertices {
		if u.master == worker.node {
			sum := make([]interface{}, worker.localSc)
			collected := make([]bool, worker.cube.NperL)
			for i := range collected {
				collected[i] = false
			}
			for _, e := range worker.edges {
				if e.s == vid {
					var v VertexData
					if e.mirror {
						if !collected[worker.vertices[e.t].master] {
							// Pull data
							resp := make(chan *[]interface{})
							worker.cube.SendInternal(worker.layer_base + worker.vertices[e.t].master,
								CmdMirrorVertexPull{globalId: u.i, resp: resp, g: cmd.g, sum: cmd.sum})
							tmp := <-resp
							for i := range *tmp {
								sum[i] = cmd.sum(sum[i], (*tmp)[i])
							}
						}
					} else {
						// Local edge
						v = *worker.vertices[e.t].data
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
	if worker.cube.sync {
		cmd.done <- nil
	}
}