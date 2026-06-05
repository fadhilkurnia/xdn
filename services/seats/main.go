// SEATS (Stonebraker Electronic Airline Ticketing System) benchmark as a
// single-container XDN service.
//
// Implements a consolidated version of the SEATS schema (airport / flight /
// customer / reservation) and its six canonical transactions: FindFlights,
// FindOpenSeats, NewReservation, UpdateReservation, DeleteReservation, and
// UpdateCustomer, over SQLite.
//
// The dataset is generated deterministically and no transaction reads a
// server-side clock or random source, so the service can be replicated under
// XDN with `--deterministic=true`.
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultAirports  = 10
	defaultCustomers = 1000
	defaultSeats     = 100
	departDate       = "2026-01-01"
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
	db, err = sql.Open("sqlite3", "file:data/seats.db?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	db.SetMaxOpenConns(1) // SQLite single writer; serialize for correctness
	if err := initSchema(); err != nil {
		log.Fatalf("failed to init schema: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/init_db", handleInitDB)
	mux.HandleFunc("/find_flights", handleFindFlights)
	mux.HandleFunc("/find_open_seats", handleFindOpenSeats)
	mux.HandleFunc("/new_reservation", handleNewReservation)
	mux.HandleFunc("/update_reservation", handleUpdateReservation)
	mux.HandleFunc("/delete_reservation", handleDeleteReservation)
	mux.HandleFunc("/update_customer", handleUpdateCustomer)

	log.Printf("seats server running on :%s\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), mux))
}

func initSchema() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS airport (
			id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT, city TEXT);
		CREATE TABLE IF NOT EXISTS flight (
			id INTEGER PRIMARY KEY, airline TEXT,
			depart_airport TEXT, arrive_airport TEXT, depart_date TEXT,
			total_seats INTEGER, seats_left INTEGER, price REAL);
		CREATE TABLE IF NOT EXISTS customer (
			id INTEGER PRIMARY KEY, name TEXT, balance REAL);
		CREATE TABLE IF NOT EXISTS reservation (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			customer_id INTEGER, flight_id INTEGER, seat INTEGER, price REAL,
			UNIQUE(flight_id, seat));
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

// ----- handlers --------------------------------------------------------------

func handleHealth(w http.ResponseWriter, r *http.Request) {
	host, _ := os.Hostname()
	writeJSON(w, http.StatusOK, map[string]any{"health": true, "host": host})
}

// handleInitDB (re)creates and populates the dataset deterministically:
// airports AP1..APa, one flight for each ordered pair of distinct airports,
// and customers cust1..custc. Reservations start empty.
func handleInitDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NumAirports    int `json:"num_airports"`
		NumCustomers   int `json:"num_customers"`
		SeatsPerFlight int `json:"seats_per_flight"`
	}
	if r.Method == http.MethodPost {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	a, c, s := req.NumAirports, req.NumCustomers, req.SeatsPerFlight
	if a <= 0 {
		a = defaultAirports
	}
	if c <= 0 {
		c = defaultCustomers
	}
	if s <= 0 {
		s = defaultSeats
	}

	start := time.Now()
	tx, err := db.Begin()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer tx.Rollback()
	for _, stmt := range []string{"DELETE FROM reservation", "DELETE FROM flight", "DELETE FROM customer", "DELETE FROM airport"} {
		if _, err := tx.Exec(stmt); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	for i := 1; i <= a; i++ {
		if _, err := tx.Exec("INSERT INTO airport (id, code, name, city) VALUES (?, ?, ?, ?)",
			i, fmt.Sprintf("AP%d", i), fmt.Sprintf("Airport %d", i), fmt.Sprintf("City %d", i)); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	flightID := 0
	for i := 1; i <= a; i++ {
		for j := 1; j <= a; j++ {
			if i == j {
				continue
			}
			flightID++
			price := 100.0 + float64((i*31+j*7)%400) // deterministic price
			if _, err := tx.Exec(`INSERT INTO flight
				(id, airline, depart_airport, arrive_airport, depart_date, total_seats, seats_left, price)
				VALUES (?, 'XX', ?, ?, ?, ?, ?, ?)`,
				flightID, fmt.Sprintf("AP%d", i), fmt.Sprintf("AP%d", j), departDate, s, s, price); err != nil {
				writeErr(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
	}
	for i := 1; i <= c; i++ {
		if _, err := tx.Exec("INSERT INTO customer (id, name, balance) VALUES (?, ?, ?)",
			i, fmt.Sprintf("cust%d", i), 10000.0); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	if err := tx.Commit(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"airports": a, "flights": flightID, "customers": c, "seats_per_flight": s,
		"took_ms": time.Since(start).Milliseconds(),
	})
}

// FindFlights (read-only): flights from depart to arrive airport, optional date.
func handleFindFlights(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Depart string `json:"depart"`
		Arrive string `json:"arrive"`
		Date   string `json:"date"`
	}
	if !decode(w, r, &req) {
		return
	}
	q := "SELECT id, airline, depart_airport, arrive_airport, depart_date, seats_left, price FROM flight WHERE depart_airport=? AND arrive_airport=?"
	args := []any{req.Depart, req.Arrive}
	if req.Date != "" {
		q += " AND depart_date=?"
		args = append(args, req.Date)
	}
	rows, err := db.Query(q, args...)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	flights := []map[string]any{}
	for rows.Next() {
		var id, seatsLeft int
		var airline, da, aa, dd string
		var price float64
		if err := rows.Scan(&id, &airline, &da, &aa, &dd, &seatsLeft, &price); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		flights = append(flights, map[string]any{
			"flight_id": id, "airline": airline, "depart": da, "arrive": aa,
			"date": dd, "seats_left": seatsLeft, "price": price,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"flights": flights, "count": len(flights)})
}

// FindOpenSeats (read-only): seat numbers not yet reserved on a flight.
func handleFindOpenSeats(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FlightID int `json:"flight_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	var total int
	if err := db.QueryRow("SELECT total_seats FROM flight WHERE id=?", req.FlightID).Scan(&total); err != nil {
		writeErr(w, http.StatusNotFound, "unknown flight")
		return
	}
	rows, err := db.Query("SELECT seat FROM reservation WHERE flight_id=?", req.FlightID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	taken := map[int]bool{}
	for rows.Next() {
		var s int
		_ = rows.Scan(&s)
		taken[s] = true
	}
	open := []int{}
	for s := 1; s <= total; s++ {
		if !taken[s] {
			open = append(open, s)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"flight_id": req.FlightID, "open_seats": open, "open_count": len(open)})
}

// NewReservation: book a specific seat for a customer on a flight.
func handleNewReservation(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID int `json:"customer_id"`
		FlightID   int `json:"flight_id"`
		Seat       int `json:"seat"`
	}
	if !decode(w, r, &req) {
		return
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		var exists int
		if err := tx.QueryRow("SELECT 1 FROM customer WHERE id=?", req.CustomerID).Scan(&exists); err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown customer")
		}
		var seatsLeft int
		var price float64
		if err := tx.QueryRow("SELECT seats_left, price FROM flight WHERE id=?", req.FlightID).Scan(&seatsLeft, &price); err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown flight")
		}
		if seatsLeft <= 0 {
			return nil, http.StatusConflict, fmt.Errorf("flight is full")
		}
		// UNIQUE(flight_id, seat) rejects a double-booked seat.
		if _, err := tx.Exec("INSERT INTO reservation (customer_id, flight_id, seat, price) VALUES (?, ?, ?, ?)",
			req.CustomerID, req.FlightID, req.Seat, price); err != nil {
			return nil, http.StatusConflict, fmt.Errorf("seat already booked")
		}
		if _, err := tx.Exec("UPDATE flight SET seats_left = seats_left - 1 WHERE id=?", req.FlightID); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		var rid int64
		_ = tx.QueryRow("SELECT id FROM reservation WHERE flight_id=? AND seat=?", req.FlightID, req.Seat).Scan(&rid)
		return map[string]any{"reservation_id": rid, "flight_id": req.FlightID, "seat": req.Seat, "price": price}, http.StatusOK, nil
	})
}

// UpdateReservation: move an existing reservation to a different seat.
func handleUpdateReservation(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ReservationID int `json:"reservation_id"`
		Seat          int `json:"seat"`
	}
	if !decode(w, r, &req) {
		return
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		var flightID int
		if err := tx.QueryRow("SELECT flight_id FROM reservation WHERE id=?", req.ReservationID).Scan(&flightID); err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown reservation")
		}
		if _, err := tx.Exec("UPDATE reservation SET seat=? WHERE id=?", req.Seat, req.ReservationID); err != nil {
			return nil, http.StatusConflict, fmt.Errorf("seat already booked")
		}
		return map[string]any{"reservation_id": req.ReservationID, "seat": req.Seat}, http.StatusOK, nil
	})
}

// DeleteReservation: cancel a reservation and free its seat.
func handleDeleteReservation(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ReservationID int `json:"reservation_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		var flightID int
		if err := tx.QueryRow("SELECT flight_id FROM reservation WHERE id=?", req.ReservationID).Scan(&flightID); err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown reservation")
		}
		if _, err := tx.Exec("DELETE FROM reservation WHERE id=?", req.ReservationID); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		if _, err := tx.Exec("UPDATE flight SET seats_left = seats_left + 1 WHERE id=?", flightID); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return map[string]any{"deleted": req.ReservationID, "flight_id": flightID}, http.StatusOK, nil
	})
}

// UpdateCustomer: update a customer's name and/or balance.
func handleUpdateCustomer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID int      `json:"customer_id"`
		Name       *string  `json:"name"`
		Balance    *float64 `json:"balance"`
	}
	if !decode(w, r, &req) {
		return
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		var name string
		var balance float64
		if err := tx.QueryRow("SELECT name, balance FROM customer WHERE id=?", req.CustomerID).Scan(&name, &balance); err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown customer")
		}
		if req.Name != nil {
			name = *req.Name
		}
		if req.Balance != nil {
			balance = *req.Balance
		}
		if _, err := tx.Exec("UPDATE customer SET name=?, balance=? WHERE id=?", name, balance, req.CustomerID); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return map[string]any{"customer_id": req.CustomerID, "name": name, "balance": balance}, http.StatusOK, nil
	})
}
