package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

const randSeed = 313

// global variables, configurable via environment variable.
var port = "80"
var stateDir = "data"
var initSize = 1      // in bytes
var execTime = 2      // in ms
var statediffSize = 2 // in bytes

// filename of the state file
var stateFile string
var openedFile *os.File

// pre-prepared written state for each request
var buffer []byte

func init() {
	rand.Seed(randSeed)
}

func main() {
	// initialize configurations: port, stateDir, initSize
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}
	if fn := os.Getenv("STATEDIR"); fn != "" {
		stateDir = fn
	}
	if szStr := os.Getenv("INITSIZE"); szStr != "" {
		sz, err := strconv.Atoi(szStr)
		if err != nil {
			log.Fatalf("INITSIZE=%s, cannot be converted to integer: %s\n",
				szStr, err.Error())
		}
		initSize = sz
	}
	if execTimeStr := os.Getenv("EXEC_TIME_MS"); execTimeStr != "" {
		et, err := strconv.Atoi(execTimeStr)
		if err != nil {
			log.Fatalf("EXEC_TIME_MS=%s, cannot be converted to integer: %s\n",
				execTimeStr, err.Error())
		}
		execTime = et
	}
	if sdSizeStr := os.Getenv("STATEDIFF_SIZE_BYTES"); sdSizeStr != "" {
		sdSize, err := strconv.Atoi(sdSizeStr)
		if err != nil {
			log.Fatalf(
				"STATEDIFF_SIZE_BYTES=%s, cannot be converted to integer: %s\n",
				sdSizeStr, err.Error())
		}
		statediffSize = sdSize
	}
	buffer = make([]byte, statediffSize)
	rand.Read(buffer)

	log.Printf("running in port=%s\n", port)
	log.Printf("running with state directory in %s\n", stateDir)
	log.Printf("running by initializing state of size %d MB\n", initSize)
	log.Printf("running with HTTP request exec time of %d ms\n", execTime)
	log.Printf("running with stateDiff size of %d bytes\n", statediffSize)

	// initialize dir for the state file
	if stateDir != "" {
		if err := os.MkdirAll(stateDir, os.ModePerm); err != nil {
			log.Fatalf("failed to initialize directory for state: %s\n",
				err.Error())
		}
	}

	// initialize the state file, deterministically
	stateFile = fmt.Sprintf("%s/state.bin", stateDir)
	initBytes := make([]byte, initSize)
	rand.Read(initBytes)

	// opening the state file
	var err error
	openedFile, err = os.OpenFile(stateFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("failed to open the state file: %s\n", err.Error())
	}
	count, err := openedFile.WriteAt(initBytes, 0)
	if err != nil {
		log.Fatalf("failed to initialize state file: %s\n",
			err.Error())
	}
	if count != initSize {
		log.Fatalf("failed to initialize state file with %d bytes, only %d\n",
			initSize, count)
	}

	// prepare http request handler
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleRequest)

	// start the web service
	log.Printf("Starting the web service in :%s\n", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%s", port), mux); err != nil {
		log.Fatalf("failed to start web service: %s\n", err.Error())
	}
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w,
			"please use POST request",
			http.StatusNotFound)
		return
	}

	startTime := time.Now()
	defer func() {
		log.Printf(". execution time: %v\n", time.Since(startTime))
	}()

	// execute for execTime ms
	time.Sleep(time.Duration(execTime) * time.Millisecond)

	// write []bytes by appending the file
	openedFile.Write(buffer)

	// sync the file
	err := openedFile.Sync()
	if err != nil {
		http.Error(w,
			"failed to sync the write",
			http.StatusInternalServerError)
		return
	}

	w.Write([]byte("ok\n"))
}
