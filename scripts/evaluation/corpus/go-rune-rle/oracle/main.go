package main

import (
	"os"
	"strconv"
)

func formatRun(char rune, count int) string {
	hex := []byte(strconv.FormatInt(int64(char), 16))
	for i, digit := range hex {
		if digit >= 'a' && digit <= 'f' {
			hex[i] = digit - 'a' + 'A'
		}
	}
	code := string(hex)
	for len(code) < 4 {
		code = "0" + code
	}
	return code + ":" + strconv.Itoa(count)
}

func encode(text string) string {
	result := ""
	var previous rune
	count := 0
	for _, current := range text {
		if count > 0 && previous != current {
			if result != "" {
				result += ","
			}
			result += formatRun(previous, count)
			count = 0
		}
		previous = current
		count++
	}
	if count > 0 {
		if result != "" {
			result += ","
		}
		result += formatRun(previous, count)
	}
	return result
}

func main() {
	if len(os.Args) != 2 {
		_, _ = os.Stdout.WriteString("error\n")
		os.Exit(2)
	}
	_, _ = os.Stdout.WriteString(encode(os.Args[1]) + "\n")
}
