package main

import (
	"CUBE"
	"flag"
	"time"
	"github.com/pkg/profile"
)

const testIter = 3
func main() {
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

	defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	begin := time.Now()
	for i := 0; i < testIter; i++ {
		cube.Sink(F1)
		cube.UpdateEdge(F2)
		cube.Push(F3, F4, sum)
		cube.Pull(F3, F4, sum)
	}
	cube.Wait()
	end := time.Now()
	println("Cost ", (end.Sub(begin) / testIter).String())
	println(cube.RpcCount.Internal / testIter, "packets,", cube.RpcCount.InternalSize / testIter, "unit")
}