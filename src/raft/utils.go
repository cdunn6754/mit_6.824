package raft

import (
	"math/rand"
	"time"
)

// Generate a timeout duration randomly spaced between [0.5, 1) seconds
func getTimeToSleep() time.Duration {
	return time.Millisecond*500 + time.Duration(rand.Intn(500))*time.Millisecond
}
