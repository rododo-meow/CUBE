package CUBE

type Data interface {
	Size() int
	Dump() string
}

type VertexData struct {
	Share Data
	Colle []Data
}

type EdgeData struct {
	Share Data
	Colle []Data
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (cube *CUBE) LowerBound(i int) int {
	return i * (cube.Sc / cube.L) + min(i, cube.Sc % cube.L)
}

func (cube *CUBE) SplitVertexData(data *VertexData, i int) *VertexData {
	ret := new(VertexData)
	ret.Share = data.Share
	ret.Colle = make([]Data, cube.LowerBound(i + 1) - cube.LowerBound(i))
	copy(ret.Colle, data.Colle[cube.LowerBound(i):cube.LowerBound(i + 1)])
	return ret
}

func (cube *CUBE) SplitEdgeData(data *EdgeData, i int) *EdgeData {
	ret := new(EdgeData)
	ret.Share = data.Share
	ret.Colle = make([]Data, cube.LowerBound(i + 1) - cube.LowerBound(i))
	copy(ret.Colle, data.Colle[cube.LowerBound(i):cube.LowerBound(i + 1)])
	return ret
}

func (cube *CUBE) CombineVertexData(data []*VertexData) *VertexData {
	var combinedData VertexData
	combinedData.Share = data[0].Share
	combinedData.Colle = []Data{}
	for _, d := range data {
		combinedData.Colle = append(combinedData.Colle, d.Colle...)
	}
	return &combinedData
}

func (cube *CUBE) CombineEdgeData(data []*EdgeData) *EdgeData {
	var combinedData EdgeData
	combinedData.Share = data[0].Share
	combinedData.Colle = []Data{}
	for _, d := range data {
		combinedData.Colle = append(combinedData.Colle, d.Colle...)
	}
	return &combinedData
}