package CUBE

func (worker *Worker) cmdPush(cmd CmdPush) {
	for vid, v := range worker.vertices {
		if v.master == worker.node {
			sum := make([]interface{}, worker.localSc)
			if len(v.mirrors) != 0 {
				// High-degree vertex, need to collect more data from mirror vertices
				for i := 0; i < len(v.mirrors) / 2; i++ {
					resp := make(chan *[]interface{})
					worker.cube.SendInternal(v.mirrors[i * 2],
						CmdMirrorVertexPush{localId: v.mirrors[i * 2 + 1], resp: resp})
					tmp := <-resp
					for i, s := range *tmp {
						sum[i] = cmd.sum(sum[i], s)
					}
				}
			}
			for _, e := range worker.edges {
				if e.t == vid {
					var u *VertexData
					if worker.vertices[e.s].master == worker.node || worker.vertices[e.s].mirror {
						// Local vertex
						uvertex := worker.vertices[e.s]
						u = uvertex.data
					} else {
						// Pull data
						resp := make(chan *VertexData)
						worker.cube.SendInternal(worker.layer_base + worker.vertices[e.s].master,
							CmdFetchVertex{i: worker.vertices[e.s].localId, resp: resp})
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
	if worker.cube.sync {
		cmd.done <- nil
	}
}