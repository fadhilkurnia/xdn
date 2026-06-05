# ecommerce - webshop OLTP web service

A single-container e-commerce / webshop OLTP service, exposed over HTTP and
backed by SQLite. It models products, customers, per-customer carts, and placed
orders, with the usual shopping transactions.

All transactions are **deterministic** given the request (fixed initial catalog,
orders numbered by an auto-increment sequence rather than timestamps, no
server-side randomness), so the service can be replicated under XDN with
`--deterministic=true`. It also ships a small browser UI at `/`.

Tech: Go, SQLite.

## Endpoints

| Path | Method | Body | Description |
|------|--------|------|-------------|
| `/health` | GET | — | liveness check |
| `/init_db` | POST | `{"num_products":100,"num_customers":1000}` | (re)create + populate catalog (optional body) |
| `/products` | POST | `{"limit":50}` or `{"product_id":1}` | read-only: browse the catalog |
| `/add_to_cart` | POST | `{"customer_id":1,"product_id":1,"qty":2}` | add to cart (checks stock) |
| `/view_cart` | POST | `{"customer_id":1}` | read-only: cart contents + total |
| `/checkout` | POST | `{"customer_id":1}` | place an order: debits balance, decrements stock, clears cart |
| `/orders` | POST | `{"customer_id":1}` | read-only: order history |
| `/restock` | POST | `{"product_id":1,"qty":50}` | increase a product's stock |

`/init_db` creates products `Product 1`..`Product p` (stock 100 each) and
customers `cust1`..`custc` (balance 10000 each).

## Run locally

```bash
docker run -p 80:80 fadhilkurnia/xdn-ecommerce
curl -X POST localhost/init_db -d '{"num_products":20,"num_customers":100}'
curl -X POST localhost/add_to_cart -d '{"customer_id":1,"product_id":1,"qty":2}'
curl -X POST localhost/checkout -d '{"customer_id":1}'
```

State (the SQLite database) is stored under `/app/data/`.

## Build a container image

```bash
docker build --tag=xdn-ecommerce .
docker buildx build --platform linux/amd64,linux/arm64 --tag=xdn-ecommerce .
```
