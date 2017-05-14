package main

import (
	"CUBE"
	"strconv"
)

type DShare struct {
	Rate float64
	Err  float64
}

func (_ DShare) Size() int {
	return 16
}
func (this DShare) Dump() string {
	return "Rate=" + strconv.FormatFloat(this.Rate, 'f', 2, 64) + ", Err=" + strconv.FormatFloat(this.Err, 'f', 2, 64)
}

type DColle struct {
	v float64
}

func (_ DColle) Size() int {
	return 8
}
func (this DColle) Dump() string {
	return strconv.FormatFloat(this.v, 'f', 2, 64)
}

const D = 10
const alpha = 0.007

func F1(_, uColle CUBE.Data, _, vColle CUBE.Data, _ CUBE.Data, _ CUBE.Data) CUBE.Data {
	return DColle{v: uColle.(DColle).v * vColle.(DColle).v}
}

func F2(share CUBE.Data, colle []CUBE.Data) *CUBE.EdgeData {
	sum := 0.0
	for _, v := range colle {
		sum += v.(DColle).v
	}
	return &CUBE.EdgeData{Share: DShare{Rate: share.(DShare).Rate, Err: share.(DShare).Rate - sum}, Colle: colle}
}

func F3(vshare CUBE.Data, vcolle CUBE.Data, eshare CUBE.Data, ecolle CUBE.Data) interface{} {
	ret := eshare.(DShare).Err * vcolle.(DColle).v
	return ret
}

func F4(_ CUBE.Data, vcolle CUBE.Data, sum interface{}) CUBE.Data {
	if sum == nil {
		return vcolle
	} else {
		newvalue := vcolle.(DColle).v + alpha * (sum.(float64) - alpha * vcolle.(DColle).v)
		return DColle{v: newvalue}
	}
}

func sum(a interface{}, b interface{}) interface{} {
	if a == nil {
		return b
	} else if b == nil {
		return a
	} else {
		return a.(float64) + b.(float64)
	}
}

func F5(_ CUBE.Data, colle []CUBE.Data) CUBE.Data {
	var sum float64 = 0
	for _, d := range colle {
		sum = sum + d.(DColle).v
	}
	return DColle{v: sum}
}