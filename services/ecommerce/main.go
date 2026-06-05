// A small e-commerce (webshop) OLTP service as a single-container XDN service.
//
// Models products, customers, per-customer carts, and placed orders, with the
// usual shopping transactions (browse, add to cart, checkout, order history)
// over SQLite. The dataset is generated deterministically and no transaction
// reads a server-side clock or random source (orders are numbered by an
// auto-increment sequence, not timestamps), so the service can be replicated
// under XDN with `--deterministic=true`.
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
	defaultProducts  = 100
	defaultCustomers = 1000
	initialStock     = 100
	initialBalance   = 10000.00
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
	db, err = sql.Open("sqlite3", "file:data/ecommerce.db?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	db.SetMaxOpenConns(1) // SQLite single writer; serialize for correctness
	if err := initSchema(); err != nil {
		log.Fatalf("failed to init schema: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/init_db", handleInitDB)
	mux.HandleFunc("/products", handleProducts)
	mux.HandleFunc("/add_to_cart", handleAddToCart)
	mux.HandleFunc("/view_cart", handleViewCart)
	mux.HandleFunc("/checkout", handleCheckout)
	mux.HandleFunc("/orders", handleOrders)
	mux.HandleFunc("/restock", handleRestock)

	log.Printf("ecommerce server running on :%s\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), mux))
}

func initSchema() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS product (
			id INTEGER PRIMARY KEY, name TEXT, price REAL, stock INTEGER);
		CREATE TABLE IF NOT EXISTS customer (
			id INTEGER PRIMARY KEY, name TEXT, balance REAL);
		CREATE TABLE IF NOT EXISTS cart_item (
			customer_id INTEGER, product_id INTEGER, qty INTEGER,
			PRIMARY KEY (customer_id, product_id));
		CREATE TABLE IF NOT EXISTS orders (
			id INTEGER PRIMARY KEY AUTOINCREMENT, customer_id INTEGER, total REAL);
		CREATE TABLE IF NOT EXISTS order_item (
			order_id INTEGER, product_id INTEGER, qty INTEGER, price REAL);
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

func handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	host, _ := os.Hostname()
	writeJSON(w, http.StatusOK, map[string]any{"health": true, "host": host})
}

// handleInitDB (re)creates and populates the catalog deterministically:
// products P1..Pp with fixed prices/stock and customers cust1..custc.
func handleInitDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NumProducts  int `json:"num_products"`
		NumCustomers int `json:"num_customers"`
	}
	if r.Method == http.MethodPost {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	p, c := req.NumProducts, req.NumCustomers
	if p <= 0 {
		p = defaultProducts
	}
	if c <= 0 {
		c = defaultCustomers
	}

	start := time.Now()
	tx, err := db.Begin()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer tx.Rollback()
	for _, stmt := range []string{"DELETE FROM order_item", "DELETE FROM orders", "DELETE FROM cart_item", "DELETE FROM product", "DELETE FROM customer", "DELETE FROM sqlite_sequence WHERE name='orders'"} {
		if _, err := tx.Exec(stmt); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	for i := 1; i <= p; i++ {
		price := 10.00 + float64((i*13)%90) // deterministic price 10..99
		if _, err := tx.Exec("INSERT INTO product (id, name, price, stock) VALUES (?, ?, ?, ?)",
			i, fmt.Sprintf("Product %d", i), price, initialStock); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	for i := 1; i <= c; i++ {
		if _, err := tx.Exec("INSERT INTO customer (id, name, balance) VALUES (?, ?, ?)",
			i, fmt.Sprintf("cust%d", i), initialBalance); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	if err := tx.Commit(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"products": p, "customers": c, "initial_stock": initialStock,
		"initial_balance": initialBalance, "took_ms": time.Since(start).Milliseconds(),
	})
}

// Products (read-only): list the catalog (optionally a single product by id).
func handleProducts(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProductID int `json:"product_id"`
		Limit     int `json:"limit"`
	}
	if r.Method == http.MethodPost {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	q := "SELECT id, name, price, stock FROM product"
	var args []any
	if req.ProductID > 0 {
		q += " WHERE id = ?"
		args = append(args, req.ProductID)
	} else {
		limit := req.Limit
		if limit <= 0 {
			limit = 50
		}
		q += " ORDER BY id LIMIT ?"
		args = append(args, limit)
	}
	rows, err := db.Query(q, args...)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	products := []map[string]any{}
	for rows.Next() {
		var id, stock int
		var name string
		var price float64
		if err := rows.Scan(&id, &name, &price, &stock); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		products = append(products, map[string]any{"id": id, "name": name, "price": price, "stock": stock})
	}
	writeJSON(w, http.StatusOK, map[string]any{"products": products, "count": len(products)})
}

// AddToCart: add a quantity of a product to a customer's cart (checks stock).
func handleAddToCart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID int `json:"customer_id"`
		ProductID  int `json:"product_id"`
		Qty        int `json:"qty"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Qty <= 0 {
		req.Qty = 1
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		var exists int
		if err := tx.QueryRow("SELECT 1 FROM customer WHERE id=?", req.CustomerID).Scan(&exists); err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown customer")
		}
		var stock int
		if err := tx.QueryRow("SELECT stock FROM product WHERE id=?", req.ProductID).Scan(&stock); err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown product")
		}
		var inCart int
		_ = tx.QueryRow("SELECT qty FROM cart_item WHERE customer_id=? AND product_id=?", req.CustomerID, req.ProductID).Scan(&inCart)
		if inCart+req.Qty > stock {
			return nil, http.StatusConflict, fmt.Errorf("insufficient stock (have %d)", stock)
		}
		if _, err := tx.Exec(`INSERT INTO cart_item (customer_id, product_id, qty) VALUES (?, ?, ?)
			ON CONFLICT(customer_id, product_id) DO UPDATE SET qty = qty + excluded.qty`,
			req.CustomerID, req.ProductID, req.Qty); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return map[string]any{"customer_id": req.CustomerID, "product_id": req.ProductID, "qty_in_cart": inCart + req.Qty}, http.StatusOK, nil
	})
}

// ViewCart (read-only): a customer's cart contents and running total.
func handleViewCart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID int `json:"customer_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	rows, err := db.Query(`SELECT p.id, p.name, c.qty, p.price
		FROM cart_item c JOIN product p ON p.id = c.product_id
		WHERE c.customer_id = ? ORDER BY p.id`, req.CustomerID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	items := []map[string]any{}
	total := 0.0
	for rows.Next() {
		var id, qty int
		var name string
		var price float64
		if err := rows.Scan(&id, &name, &qty, &price); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		items = append(items, map[string]any{"product_id": id, "name": name, "qty": qty, "price": price})
		total += price * float64(qty)
	}
	writeJSON(w, http.StatusOK, map[string]any{"customer_id": req.CustomerID, "items": items, "total": total})
}

// Checkout: turn a customer's cart into an order — verify stock and balance,
// decrement stock, debit balance, record the order, and clear the cart.
func handleCheckout(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID int `json:"customer_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		var balance float64
		if err := tx.QueryRow("SELECT balance FROM customer WHERE id=?", req.CustomerID).Scan(&balance); err != nil {
			return nil, http.StatusNotFound, fmt.Errorf("unknown customer")
		}
		rows, err := tx.Query(`SELECT c.product_id, c.qty, p.price, p.stock
			FROM cart_item c JOIN product p ON p.id = c.product_id
			WHERE c.customer_id = ? ORDER BY c.product_id`, req.CustomerID)
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		type line struct {
			pid, qty int
			price    float64
		}
		var lines []line
		total := 0.0
		for rows.Next() {
			var pid, qty, stock int
			var price float64
			if err := rows.Scan(&pid, &qty, &price, &stock); err != nil {
				rows.Close()
				return nil, http.StatusInternalServerError, err
			}
			if qty > stock {
				rows.Close()
				return nil, http.StatusConflict, fmt.Errorf("insufficient stock for product %d", pid)
			}
			lines = append(lines, line{pid, qty, price})
			total += price * float64(qty)
		}
		rows.Close()
		if len(lines) == 0 {
			return nil, http.StatusConflict, fmt.Errorf("cart is empty")
		}
		if balance < total {
			return nil, http.StatusConflict, fmt.Errorf("insufficient balance (have %.2f, need %.2f)", balance, total)
		}
		res, err := tx.Exec("INSERT INTO orders (customer_id, total) VALUES (?, ?)", req.CustomerID, total)
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		orderID, _ := res.LastInsertId()
		for _, l := range lines {
			if _, err := tx.Exec("INSERT INTO order_item (order_id, product_id, qty, price) VALUES (?, ?, ?, ?)",
				orderID, l.pid, l.qty, l.price); err != nil {
				return nil, http.StatusInternalServerError, err
			}
			if _, err := tx.Exec("UPDATE product SET stock = stock - ? WHERE id=?", l.qty, l.pid); err != nil {
				return nil, http.StatusInternalServerError, err
			}
		}
		if _, err := tx.Exec("UPDATE customer SET balance = balance - ? WHERE id=?", total, req.CustomerID); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		if _, err := tx.Exec("DELETE FROM cart_item WHERE customer_id=?", req.CustomerID); err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return map[string]any{"order_id": orderID, "total": total, "balance": balance - total, "lines": len(lines)}, http.StatusOK, nil
	})
}

// Orders (read-only): a customer's order history.
func handleOrders(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID int `json:"customer_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	rows, err := db.Query("SELECT id, total FROM orders WHERE customer_id=? ORDER BY id", req.CustomerID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	orders := []map[string]any{}
	for rows.Next() {
		var id int
		var total float64
		if err := rows.Scan(&id, &total); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		orders = append(orders, map[string]any{"order_id": id, "total": total})
	}
	writeJSON(w, http.StatusOK, map[string]any{"customer_id": req.CustomerID, "orders": orders, "count": len(orders)})
}

// Restock: increase a product's available stock.
func handleRestock(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProductID int `json:"product_id"`
		Qty       int `json:"qty"`
	}
	if !decode(w, r, &req) {
		return
	}
	runWrite(w, func(tx *sql.Tx) (any, int, error) {
		res, err := tx.Exec("UPDATE product SET stock = stock + ? WHERE id=?", req.Qty, req.ProductID)
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return nil, http.StatusNotFound, fmt.Errorf("unknown product")
		}
		var stock int
		_ = tx.QueryRow("SELECT stock FROM product WHERE id=?", req.ProductID).Scan(&stock)
		return map[string]any{"product_id": req.ProductID, "stock": stock}, http.StatusOK, nil
	})
}
