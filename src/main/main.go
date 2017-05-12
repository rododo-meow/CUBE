package main

import (
	"CUBE"
	"flag"
	"time"
)

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

	/*err = load(cube, datafile)
	if err != nil {
		print(err.Error())
		return
	}*/
	loadWiki(cube, "wikiElec.ElecBs3.txt")
	//loadMtx(cube, "ash608.mtx")
	//testGraph(cube, 2 * D)

	begin := time.Now()
	for i := 0; i < 1; i++ {
		cube.Sink(F1)
		cube.UpdateEdge(F2)
		cube.Push(F3, F4, sum)
		cube.Pull(F3, F4, sum)
	}
	cube.Wait()
	end := time.Now()
	println("Cost ", end.Sub(begin).String())
	println(cube.RpcCount.Internal, "packets,", cube.RpcCount.InternalSize, "unit")
}