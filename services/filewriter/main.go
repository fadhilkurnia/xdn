package main

import (
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	mathRand "math/rand"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"
)

// global variables, configurable via environment variable.
var port = "8000"
var stateDir = "data"
var initSize = 50 // in MB

// filename of the state file
var stateFile string
var openedFile *os.File

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
	log.Printf("running in port=%s\n", port)
	log.Printf("running with state directory in %s\n", stateDir)
	log.Printf("running by initializing state of size %d MB\n", initSize)

	// initialize dir for the state file
	if stateDir != "" {
		if err := os.MkdirAll(stateDir, os.ModePerm); err != nil {
			log.Fatalf("failed to initialize directory for state: %s\n",
				err.Error())
		}
	}
	// initialize the state file:
	// dd if=/dev/urandom of=data/state.bin bs=1M count=100
	stateFile = fmt.Sprintf("%s/state.bin", stateDir)
	err := exec.Command("dd",
		"if=/dev/urandom",
		fmt.Sprintf("of=%s", stateFile),
		"bs=1M",
		fmt.Sprintf("count=%d", initSize)).Run()
	if err != nil {
		log.Fatalf("failed to initialize state file: %s\n", err.Error())
	}

	// opening the state file
	openedFile, err = os.OpenFile(stateFile, os.O_WRONLY, 0600)
	if err != nil {
		log.Fatalf("failed to open the state file: %s\n", err.Error())
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
	_ = r.ParseForm()
	log.Printf("handling %s %s, form payload:  %v\n",
		r.Method, r.URL.Path, r.Form)

	startTime := time.Now()
	defer func() {
		log.Printf(". execution time: %v\n", time.Since(startTime))
	}()

	if r.Method != http.MethodPost {
		http.Error(w,
			"please use POST request",
			http.StatusNotFound)
		return
	}

	// get the size attributes
	size := 8 // in bytes
	if szStr := r.FormValue("size"); szStr != "" {
		var err error
		size, err = strconv.Atoi(szStr)
		if err != nil {
			http.Error(w,
				"incorrect size given",
				http.StatusBadRequest)
			return
		}
		if size > initSize*1_000_000 {
			http.Error(w,
				fmt.Sprintf("size overflow, expected less than %d",
					initSize*1_000_000),
				http.StatusBadRequest)
			return
		}
	}

	// generate random data to be written
	randBytes := make([]byte, size)
	rand.Read(randBytes)

	// generate random valid offset [0..initSize-size]
	offsetBound := uint64(initSize)*1_000_000 - uint64(size) + 1
	randOffset := mathRand.Int63n(int64(offsetBound))

	// write the random data
	writeStartTime := time.Now()
	n, err := openedFile.WriteAt(randBytes, randOffset)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("failed to write: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}
	if n != len(randBytes) {
		http.Error(w,
			fmt.Sprintf(
				"failed to write the specified size=%d, written=%d",
				size,
				n),
			http.StatusInternalServerError)
		return
	}
	log.Printf(". write time: %v\n", time.Since(writeStartTime))

	// sync the file
	syncStartTime := time.Now()
	err = openedFile.Sync()
	if err != nil {
		http.Error(w,
			"failed to sync the write",
			http.StatusInternalServerError)
		return
	}
	log.Printf(". sync time: %v\n", time.Since(syncStartTime))

	// get checksum of the updated file, note that this require
	// linear scanning of the file, thus this can take some time.
	// checksum := getFileChecksum(openedFile.Name())
	// log.Printf(". checksum: %s\n", checksum)

	w.Write([]byte("ok\n"))
}

func getFileChecksum(filePath string) string {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
		return ""
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
		return ""
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}
