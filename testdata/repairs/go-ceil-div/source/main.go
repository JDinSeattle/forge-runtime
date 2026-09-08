package main

import (
	"os"
	"strconv"
)

// ceilDiv returns the smallest integer at least value/divisor.
// Negative values and nonpositive divisors are invalid.
func ceilDiv(value, divisor int64) (int64, bool) {
	if value < 0 || divisor <= 0 {
		return 0, false
	}
	return value/divisor + 1, true
}

func main() {
	if len(os.Args) != 3 {
		fail()
	}
	value, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		fail()
	}
	divisor, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil {
		fail()
	}
	answer, ok := ceilDiv(value, divisor)
	if !ok {
		fail()
	}
	_, _ = os.Stdout.WriteString(strconv.FormatInt(answer, 10) + "\n")
}

func fail() {
	_, _ = os.Stdout.WriteString("error\n")
	os.Exit(2)
}
