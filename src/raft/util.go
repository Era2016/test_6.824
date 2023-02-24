package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		logger := log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)
		logger.Printf(format, a...)
		//log.Printf(format, a...)
	}
	return
}
