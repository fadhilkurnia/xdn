// SmallBank OLTP benchmark as a single-container XDN service.
//
// Implements the canonical SmallBank schema (account / savings / checking) and
// its six transactions (Balance, DepositChecking, TransactSavings, Amalgamate,
// WriteCheck, SendPayment) over SQLite. All transactions are deterministic
// given the request — initial balances are fixed, and there is no server-side
// clock or randomness in stored state — so the service can run under XDN with
// `--deterministic=true`.
package main

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed index.html
var indexHTML []byte

const (
	defaultAccounts = 1000
	initialBalance  = 1000.00 // starting savings and checking balance per account
	checkPenalty    = 1.00    // WriteCheck penalty when balance is insufficient
)

var db *sql.DB

func main() {
	port := "80"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}
	if err := os.MkdirAll("data", 0o755); err != nil {
		log.Fatalf("failed to create state dir: %v", err)
	}

	var err error
	db, err = sql.Open("sqlite3", "file:data/smallbank.db?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	// SQLite allows a single writer; serialize to keep transactions correct.
	db.SetMaxOpenConns(1)
	if err := initSchema(); err != nil {
		log.Fatalf("failed to init schema: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/init_db", handleInitDB)
	mux.HandleFunc("/balance", handleBalance)
	mux.HandleFunc("/deposit_checking", handleDepositChecking)
	mux.HandleFunc("/transact_savings", handleTransactSavings)
	mux.HandleFunc("/amalgamate", handleAmalgamate)
	mux.HandleFunc("/write_check", handleWriteCheck)
	mux.HandleFunc("/send_payment", handleSendPayment)

	log.Printf("smallbank server running on :%s\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), mux))
}

func initSchema() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS account (custid INTEGER PRIMARY KEY, name TEXT UNIQUE);
		CREATE TABLE IF NOT EXISTS savings (custid INTEGER PRIMARY KEY, balance REAL);
		CREATE TABLE IF NOT EXISTS checking (custid INTEGER PRIMARY KEY, balance REAL);
	`)
	return err
}

// ----- helpers ---------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "use POST")
		return false
	}
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

// custID looks up a customer id by account name within a transaction.
func custID(tx *sql.Tx, name string) (int64, error) {
	var id int64
	err := tx.QueryRow("SELECT custid FROM account WHERE name = ?", name).Scan(&id)
	return id, err
}

// ----- handlers --------------------------------------------------------------

// handleIndex serves the browser UI at "/" (and any unmatched path).
func handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	host, _ := os.Hostname()
	writeJSON(w, http.StatusOK, map[string]any{"health": true, "host": host})
}

// handleInitDB (re)creates and populates the dataset with deterministic
// initial balances: customer i (1..N) is named "cust<i>" with savings and
// checking both set to initialBalance.
func handleInitDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NumAccounts int `json:"num_accounts"`
	}
	// Body is optional; default the size if not provided.
	if r.Method == http.MethodPost {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	n := req.NumAccounts
	if n <= 0 {
		n = defaultAccounts
	}

	start := time.Now()
	tx, err := db.Begin()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer tx.Rollback()
	for _, stmt := range []string{"DELETE FROM account", "DELETE FROM savings", "DELETE FROM checking"} {
		if _, err := tx.Exec(stmt); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	accStmt, _ := tx.Prepare("INSERT INTO account (custid, name) VALUES (?, ?)")
	savStmt, _ := tx.Prepare("INSERT INTO savings (custid, balance) VALUES (?, ?)")
	chkStmt, _ := tx.Prepare("INSERT INTO checking (custid, balance) VALUES (?, ?)")
	defer accStmt.Close()
	defer savStmt.Close()
	defer chkStmt.Close()
	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("cust%d", i)
		if _, err := accStmt.Exec(i, name); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if _, err := savStmt.Exec(i, initialBalance); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if _, err := chkStmt.Exec(i, initialBalance); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	if err := tx.Commit(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"initialized": n, "initial_balance": initialBalance, "took_ms": time.Since(start).Milliseconds(),
	})
}

// Balance (read-only): total of savings + checking for a customer.
func handleBalance(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if !decode(w, r, &req) {
		return
	}
	tx, err := db.Begin()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer tx.Rollback()
	id, err := custID(tx, req.Name)
	if err != nil {
		writeErr(w, http.StatusNotFound, "unknown account: "+req.Name)
		return
	}
	var sav, chk float64
	if err := tx.QueryRow("SELECT balance FROM savings WHERE custid=?", id).Scan(&sav); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := tx.QueryRow("SELECT balance FROM checking WHERE custid=?", id).Scan(&chk); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": req.Name, "savings": sav, "checking": chk, "total": sav + chk})
}

// DepositChecking: add amount to a customer's checking balance.
func handleDepositChecking(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string  `json:"name"`
		Amount float64 `json:"amount"`
	}
	if !decode(w, r, &req) {
		return
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		id, err := custID(tx, req.Name)
		if err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown account: %s", req.Name)
		}
		if _, err := tx.Exec("UPDATE checking SET balance = balance + ? WHERE custid=?", req.Amount, id); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return map[string]any{"name": req.Name, "deposited": req.Amount}, http.StatusOK, nil
	})
}

// TransactSavings: add amount (may be negative) to savings; reject overdraft.
func handleTransactSavings(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string  `json:"name"`
		Amount float64 `json:"amount"`
	}
	if !decode(w, r, &req) {
		return
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		id, err := custID(tx, req.Name)
		if err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown account: %s", req.Name)
		}
		var sav float64
		if err := tx.QueryRow("SELECT balance FROM savings WHERE custid=?", id).Scan(&sav); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		if sav+req.Amount < 0 {
			return nil, http.StatusConflict, fmt.Errorf("insufficient savings")
		}
		if _, err := tx.Exec("UPDATE savings SET balance = balance + ? WHERE custid=?", req.Amount, id); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return map[string]any{"name": req.Name, "savings": sav + req.Amount}, http.StatusOK, nil
	})
}

// Amalgamate: move all funds from name1 (savings+checking) into name2's
// checking, then zero out name1.
func handleAmalgamate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name1 string `json:"name1"`
		Name2 string `json:"name2"`
	}
	if !decode(w, r, &req) {
		return
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		id1, err := custID(tx, req.Name1)
		if err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown account: %s", req.Name1)
		}
		id2, err := custID(tx, req.Name2)
		if err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown account: %s", req.Name2)
		}
		var sav1, chk1 float64
		if err := tx.QueryRow("SELECT balance FROM savings WHERE custid=?", id1).Scan(&sav1); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		if err := tx.QueryRow("SELECT balance FROM checking WHERE custid=?", id1).Scan(&chk1); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		total := sav1 + chk1
		if _, err := tx.Exec("UPDATE savings SET balance = 0 WHERE custid=?", id1); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		if _, err := tx.Exec("UPDATE checking SET balance = 0 WHERE custid=?", id1); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		if _, err := tx.Exec("UPDATE checking SET balance = balance + ? WHERE custid=?", total, id2); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return map[string]any{"from": req.Name1, "to": req.Name2, "moved": total}, http.StatusOK, nil
	})
}

// WriteCheck: decrease checking by amount; if total balance is insufficient,
// apply a penalty.
func handleWriteCheck(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string  `json:"name"`
		Amount float64 `json:"amount"`
	}
	if !decode(w, r, &req) {
		return
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		id, err := custID(tx, req.Name)
		if err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown account: %s", req.Name)
		}
		var sav, chk float64
		if err := tx.QueryRow("SELECT balance FROM savings WHERE custid=?", id).Scan(&sav); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		if err := tx.QueryRow("SELECT balance FROM checking WHERE custid=?", id).Scan(&chk); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		amount := req.Amount
		penalized := false
		if sav+chk < amount {
			amount += checkPenalty
			penalized = true
		}
		if _, err := tx.Exec("UPDATE checking SET balance = balance - ? WHERE custid=?", amount, id); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return map[string]any{"name": req.Name, "charged": amount, "penalized": penalized}, http.StatusOK, nil
	})
}

// SendPayment: move amount from sender's checking to recipient's checking;
// reject if the sender has insufficient checking funds.
func handleSendPayment(w http.ResponseWriter, r *http.Request) {
	var req struct {
		From   string  `json:"from"`
		To     string  `json:"to"`
		Amount float64 `json:"amount"`
	}
	if !decode(w, r, &req) {
		return
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		idFrom, err := custID(tx, req.From)
		if err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown account: %s", req.From)
		}
		idTo, err := custID(tx, req.To)
		if err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown account: %s", req.To)
		}
		var chkFrom float64
		if err := tx.QueryRow("SELECT balance FROM checking WHERE custid=?", idFrom).Scan(&chkFrom); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		if chkFrom < req.Amount {
			return nil, http.StatusConflict, fmt.Errorf("insufficient checking funds")
		}
		if _, err := tx.Exec("UPDATE checking SET balance = balance - ? WHERE custid=?", req.Amount, idFrom); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		if _, err := tx.Exec("UPDATE checking SET balance = balance + ? WHERE custid=?", req.Amount, idTo); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return map[string]any{"from": req.From, "to": req.To, "amount": req.Amount}, http.StatusOK, nil
	})
}

// runWrite wraps a write transaction: begin, run fn, commit on success.
func runWrite(w http.ResponseWriter, fn func(tx *sql.Tx) (any, int, error)) {
	tx, err := db.Begin()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer tx.Rollback()
	resp, code, err := fn(tx)
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	if err := tx.Commit(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, code, resp)
}
