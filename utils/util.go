package utils

import (
	"time"
)

// Max of two int
func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// Max of two int
func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// VMax of a vector
func VMax(v ...int) int {
	max := v[0]
	for _, i := range v {
		if max < i {
			max = i
		}
	}
	return max
}

// Schedule repeatedly call function with intervals
func Schedule(what func(), delay time.Duration) chan bool {
	stop := make(chan bool)

	go func() {
		for {
			what()
			select {
			case <-time.After(delay):
			case <-stop:
				return
			}
		}
	}()

	return stop
}

func IntInSlice(needle int, haystack []int) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}