package main

import (
	"CUBE"
	"os"
	"bufio"
	"fmt"
	"math/rand"
)

func loadWiki(cube *CUBE.CUBE, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		print("Can't open " + os.Args[1])
		return err
	}
	reader := bufio.NewScanner(f)

	vertices := make(map[int]bool)
	var U int
	ne := 0
	for reader.Scan() {
		line := reader.Text()
		if len(line) == 0 {
			continue
		}
		if line[0] == 'U' {
			_, err := fmt.Sscanf(line, "U %d", &U)
			if err != nil {
				return err
			}
			if _, ok := vertices[U]; !ok {
				vertices[U] = true
			}
		} else if line[0] == 'V' {
			var v, s int
			_, err := fmt.Sscanf(line, "V %d %d", &v, &s)
			if err != nil {
				return err
			}

			dcolle := make([]CUBE.Data, D)
			for i := 0; i < D; i++ {
				dcolle[i] = DColle{v: rand.Float64()}
			}
			cube.AddEdge(s, U,
				&CUBE.EdgeData{
					Share: DShare{Rate: float64(v * D), Err: float64(0)},
					Colle: dcolle,
				})
			if _, ok := vertices[s]; !ok {
				vertices[s] = true
			}
			ne++
		}
	}
	for v, _ := range vertices {
		dcolle := make([]CUBE.Data, D)
		for i := 0; i < D; i++ {
			dcolle[i] = DColle{v: rand.Float64()}
		}
		cube.AddVertex(v, &CUBE.VertexData{
			Share: nil,
			Colle: dcolle,
		})
	}
	println("Total", len(vertices), "vertices,", ne, "edges")
	cube.FinalizeGraph(100)
	return nil
}

func testGraph(cube *CUBE.CUBE, size int) {
	for i := 0; i < size; i++ {
		for j := i + 1; j < size; j++ {
			dcolle := make([]CUBE.Data, D)
			for i := 0; i < D; i++ {
				dcolle[i] = DColle{v: rand.Float64()}
			}
			cube.AddEdge(i, j,
				&CUBE.EdgeData{
					Share: DShare{Rate: float64((rand.Intn(3) - 1) * D), Err: float64(0)},
					Colle: dcolle,
				})
		}
		dcolle := make([]CUBE.Data, D)
		for i := 0; i < D; i++ {
			dcolle[i] = DColle{v: rand.Float64()}
		}
		cube.AddVertex(i,
			&CUBE.VertexData{
				Share: nil,
				Colle: dcolle,
			})
	}
	cube.FinalizeGraph(0)
}

func loadMtx(cube *CUBE.CUBE, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		print("Can't open " + os.Args[1])
		return err
	}
	reader := bufio.NewScanner(f)
	reader.Scan()
	for len(reader.Text()) == 0 || reader.Text()[0] == '%' {
		reader.Scan()
	}

	var ns, nt, ne int
	if _, err := fmt.Sscanf(reader.Text(), "%d %d %d", &ns, &nt, &ne); err != nil {
		println("Can't parse header ", reader.Text())
		return err
	}
	for i := 0; i < ns + nt; i++ {
		dcolle := make([]CUBE.Data, D)
		for i := 0; i < D; i++ {
			dcolle[i] = DColle{v: rand.Float64()}
		}
		cube.AddVertex(i, &CUBE.VertexData{
			Share: nil,
			Colle: dcolle,
		})
	}
	for reader.Scan() {
		if len(reader.Text()) == 0 || reader.Text()[0] == '%' {
			continue
		}
		var s, t int
		if _, err := fmt.Sscanf(reader.Text(), "%d %d", &s, &t); err != nil {
			println("Can't parse edge ", reader.Text())
			return err
		}
		dcolle := make([]CUBE.Data, D)
		for i := 0; i < D; i++ {
			dcolle[i] = DColle{v: 0}
		}
		cube.AddEdge(s - 1, t - 1 + ns, &CUBE.EdgeData{
			Share: DShare{Rate: D, Err: 0},
			Colle: dcolle,
		})
	}
	cube.FinalizeGraph(0)
	return nil
}