package main

import (
	"os"
	"strconv"
)

func midpoint(lo, hi int64) int64 { return lo + int64((uint64(hi)-uint64(lo))/2) }

func main() {
	if len(os.Args) != 3 {
		_, _ = os.Stdout.WriteString("error\n")
		os.Exit(2)
	}
	lo, first := strconv.ParseInt(os.Args[1], 10, 64)
	hi, second := strconv.ParseInt(os.Args[2], 10, 64)
	if first != nil || second != nil || lo > hi {
		_, _ = os.Stdout.WriteString("error\n")
		os.Exit(2)
	}
	_, _ = os.Stdout.WriteString(strconv.FormatInt(midpoint(lo, hi), 10) + "\n")
}
