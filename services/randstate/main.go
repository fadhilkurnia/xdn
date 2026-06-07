package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// randstate is a NON-DETERMINISTIC XDN service used to demonstrate the primary-backup
// non-deterministic initial-state synchronization (and to stress its atomicity).
//
// The problem it embodies: a service generates RANDOM state on bootstrap, so if each replica
// bootstrapped independently they would DIVERGE from the very first instant. Primary-backup solves
// this by having only the PRIMARY bootstrap (its random state) and synchronizing that exact state
// to the backups. This service makes that observable:
//
//   - First bootstrap (the original primary): generate a random boot_id and a burst of random rows
//     under /app/data, then go quiescent. (A promoted backup or a restart already has boot_id from
//     the synced/persisted state, so it MUST NOT bootstrap again -- that is the divergence the
//     init-sync prevents.)
//   - /hash is the fingerprint of the whole state; all replicas must report the PRIMARY's hash.
//
// State is a plain append-only log (not a database) so the demonstration is not confounded by
// database-specific recovery such as SQLite WAL. Pure Go (CGO disabled) -> trivial multi-arch.
//
// Env:
//   STATE_DIR          state directory (default /app/data)
//   BOOTSTRAP_ROWS     number of random rows written on first bootstrap (default 200)
//   BOOTSTRAP_ROW_BYTES raw random bytes per row, hex-encoded on disk so ~2x (default 8). Bump it
//                      for a LARGE initial state, e.g. ROWS=200 ROW_BYTES=262144 -> ~100MB. NOTE:
//                      RECORDER init-sync ships the whole state in-band through the JVM/paxos and
//                      OOMs the small-heap ARs well before ~64MB; large init state needs RSYNC.
//   ALWAYS_BOOTSTRAP   "true" -> re-randomize on EVERY start (drop the skip-guard). A promoted
//                      backup overwrites the synced state, so the boot_id CHANGES on every primary
//                      change (vs the default, which preserves the original primary's state).
//   BOOTSTRAP_WRITE_MS spread the bootstrap writes over this many ms (default 0 = one fast burst,
//                      quiescent at capture). Set > the migrate-wait (~5s), e.g. 8000, to make rows
//                      land DURING the init-sync window -- useful to stress the RSYNC seam.

var (
	stateDir string
	logPath  string
	bootPath string
	mu       sync.Mutex
	bootDone int32 // 0/1 via sync/atomic
	bootID   string
)

func main() {
	stateDir = env("STATE_DIR", "/app/data")
	logPath = filepath.Join(stateDir, "state.log")
	bootPath = filepath.Join(stateDir, "boot_id")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", stateDir, err)
	}

	// ALWAYS_BOOTSTRAP=true is the PATHOLOGICAL mode: re-generate random state on EVERY start,
	// even when synced state already exists. This is the negative control -- it deliberately drops
	// the skip-guard, so a promoted backup overwrites the primary's synced state with a fresh random
	// boot_id. (Under RECORDER init-sync the new primary then re-syncs that fresh state to the
	// backups, so they stay consistent but the boot_id CHANGES on every primary change.)
	always := env("ALWAYS_BOOTSTRAP", "") == "true" || env("ALWAYS_BOOTSTRAP", "") == "1"
	if existing, err := os.ReadFile(bootPath); !always && err == nil && len(existing) > 0 {
		bootID = trimNL(string(existing))
		atomic.StoreInt32(&bootDone, 1)
		log.Printf("randstate: existing state (boot_id=%s) -- skipping bootstrap", bootID)
	} else {
		if always {
			// wipe any synced state so each bootstrap is a clean random slate
			_ = os.Remove(logPath)
			log.Printf("randstate: ALWAYS_BOOTSTRAP=true -- re-randomizing (ignoring existing state)")
		}
		bootstrap()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/state", stateHandler)
	mux.HandleFunc("/hash", hashHandler)
	mux.HandleFunc("/boot", bootHandler)
	mux.HandleFunc("/log", logHandler)
	mux.HandleFunc("/ready", readyHandler)
	mux.HandleFunc("/write", writeHandler) // POST: append one random row
	mux.HandleFunc("/", stateHandler)

	port := env("PORT", "80")
	log.Printf("randstate on :%s (stateDir=%s)", port, stateDir)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// bootstrap generates the random initial state. Only the original primary runs this.
func bootstrap() {
	bootID = randHex(16)
	if err := os.WriteFile(bootPath, []byte(bootID+"\n"), 0o644); err != nil {
		log.Fatalf("write boot_id: %v", err)
	}
	appendLine(fmt.Sprintf("boot %s %d", bootID, time.Now().UnixNano()))
	log.Printf("randstate: bootstrapping boot_id=%s", bootID)

	rows := atoiDefault(env("BOOTSTRAP_ROWS", "200"), 200)
	spreadMs := atoiDefault(env("BOOTSTRAP_WRITE_MS", "0"), 0)
	// raw random bytes per row (hex-encoded on disk, so each row is ~2x this). Bump it to make a
	// LARGE initial state, e.g. BOOTSTRAP_ROWS=200 BOOTSTRAP_ROW_BYTES=262144 -> ~100MB, to test
	// RECORDER shipping a big init diff in-band (rsync handles large state trivially; RECORDER's
	// single in-band ApplyStateDiff is the untested path).
	rowBytes := atoiDefault(env("BOOTSTRAP_ROW_BYTES", "8"), 8)

	if spreadMs <= 0 {
		// Fast synchronous burst: all random state is written before we start serving, so it is
		// quiescent by the time the init-sync captures it.
		for i := 0; i < rows; i++ {
			appendLine(fmt.Sprintf("row %d %s", i, randHex(rowBytes)))
		}
		atomic.StoreInt32(&bootDone, 1)
		log.Printf("randstate: bootstrapped %d rows (burst)", rows)
		return
	}

	// Spread writes over a window (overlapping the init-sync) to stress the RSYNC seam. These run
	// in the background; note the PB diff pipeline captures per-request, so rows landing after the
	// init capture are only guaranteed to replicate under RECORDER mode's atomic capture point.
	go func() {
		denom := rows
		if denom < 1 {
			denom = 1
		}
		gap := time.Duration(spreadMs) * time.Millisecond / time.Duration(denom)
		for i := 0; i < rows; i++ {
			appendLine(fmt.Sprintf("row %d %s", i, randHex(rowBytes)))
			time.Sleep(gap)
		}
		atomic.StoreInt32(&bootDone, 1)
		log.Printf("randstate: bootstrapped %d rows over %dms", rows, spreadMs)
	}()
}

func appendLine(s string) {
	mu.Lock()
	defer mu.Unlock()
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("append: %v", err)
		return
	}
	defer f.Close()
	if _, err := f.WriteString(s + "\n"); err != nil {
		log.Printf("write: %v", err)
		return
	}
	_ = f.Sync() // flush through the (fuselog) filesystem so the write is durable + captured
}

func readState() (string, []byte) {
	mu.Lock()
	defer mu.Unlock()
	d, _ := os.ReadFile(logPath)
	return bootID, d
}

func stateHandler(w http.ResponseWriter, r *http.Request) {
	id, data := readState()
	sum := sha256.Sum256(data)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"boot_id": id,
		"rows":    countLines(data),
		"bytes":   len(data),
		"hash":    hex.EncodeToString(sum[:]),
		"ready":   atomic.LoadInt32(&bootDone) == 1,
	})
}

func hashHandler(w http.ResponseWriter, r *http.Request) {
	_, data := readState()
	sum := sha256.Sum256(data)
	fmt.Fprintln(w, hex.EncodeToString(sum[:]))
}

func bootHandler(w http.ResponseWriter, r *http.Request) {
	id, _ := readState()
	fmt.Fprintln(w, id)
}

func logHandler(w http.ResponseWriter, r *http.Request) {
	_, data := readState()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(data)
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt32(&bootDone) == 1 {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("bootstrapping\n"))
}

func writeHandler(w http.ResponseWriter, r *http.Request) {
	appendLine(fmt.Sprintf("write %d %s", time.Now().UnixNano(), randHex(8)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func atoiDefault(s string, d int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return d
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		log.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(b)
}

func trimNL(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

func countLines(b []byte) int {
	n := 0
	for _, c := range b {
		if c == '\n' {
			n++
		}
	}
	return n
}
