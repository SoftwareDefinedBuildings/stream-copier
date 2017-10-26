package main

import (
	"context"
	"fmt"
	"math"

	btrdb "gopkg.in/btrdb.v4"
)

const (
	BTrDBMinTime int64 = 1 - (16 << 56)
	BTrDBMaxTime int64 = (48 << 56) - 1
)

func CopyStream(ctx context.Context, src *btrdb.Stream, dst *btrdb.Stream, copies uint64, delta int64, spacing int64) {
	pipe := make(chan btrdb.RawPoint, 256<<20)
	go Reader(ctx, src, copies, pipe)
	Writer(ctx, dst, delta, spacing, pipe)
}

func Reader(ctx context.Context, src *btrdb.Stream, copies uint64, out chan<- btrdb.RawPoint) {
	var i uint64
	for i = 0; i != copies; i++ {
		pts, _, errc := src.RawValues(ctx, BTrDBMinTime, BTrDBMaxTime, btrdb.LatestVersion)
		for pt := range pts {
			out <- pt
		}

		err := <-errc
		if err != nil {
			panic(err)
		}

		fmt.Printf("Finished copy %v\n", i)
	}
}

func Writer(ctx context.Context, dst *btrdb.Stream, delta int64, spacing int64, in <-chan btrdb.RawPoint) {
	buffer := make([]btrdb.RawPoint, 0, 10000)
	lastTime := int64(math.MinInt64)
	for pt := range in {
		if pt.Time < lastTime {
			// A new copy has started
			delta = (lastTime + spacing) - pt.Time
		}

		pt.Time += delta
		buffer = append(buffer, pt)
		if len(buffer) >= cap(buffer) {
			err := dst.Insert(ctx, buffer)
			if err != nil {
				panic(err)
			}
			buffer = buffer[:0]
		}
	}

	if len(buffer) != 0 {
		err := dst.Insert(ctx, buffer)
		if err != nil {
			panic(err)
		}
	}
}
