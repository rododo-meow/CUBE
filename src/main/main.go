package main

import (
	"CUBE"
	"flag"
	"time"
	"github.com/pkg/profile"
)

const testIter = 3
func main() {

	defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	var N, L int
	flag.IntVar(&N, "n", 0, "num of nodes")
	flag.IntVar(&L, "l", 0, "num of layer")
	flag.Parse()

	cube, err := CUBE.CreateCUBE(N, L, D, false)
	if err != nil {
		print(err.Error())
		return
	}

	loadWiki(cube, "wikiElec.ElecBs3.txt")
	//loadMtx(cube, "ash608.mtx")
	//testGraph(cube, 2 * D)
	//loadSampleGraph(cube)
	println("Total", cube.NPureVertices, "vertices,", cube.NPureEdges, "edges")
	println("λ=", float64(cube.NVertices) / float64(cube.NPureVertices) / float64(cube.L))

	begin := time.Now()
	for i := 0; i < testIter; i++ {
		cube.Sink(F1)
		cube.UpdateEdge(F2, F5)
		cube.Push(F3, F4, sum)
		cube.Pull(F3, F4, sum)
	}
	cube.Wait()
	end := time.Now()
	println("Cost ", (end.Sub(begin) / testIter).String())
	println(cube.RpcCount.Internal / testIter, "packets,", cube.RpcCount.InternalSize / testIter, "unit")
}