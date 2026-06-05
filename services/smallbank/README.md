# smallbank - SmallBank OLTP benchmark web service

A single-container implementation of the [SmallBank](https://hstore.cs.brown.edu/projects/smallbank/)
OLTP benchmark, exposed over HTTP and backed by SQLite. Each customer has a
`savings` and a `checking` account; the workload is a mix of small money-moving
transactions.

All transactions are **deterministic** given the request (fixed initial
balances, no server-side clock or randomness in stored state), so the service
can be replicated under XDN with `--deterministic=true`.

Tech: Go, SQLite.

## Endpoints

| Path | Method | Body | Description |
|------|--------|------|-------------|
| `/health` | GET | — | liveness check |
| `/init_db` | POST | `{"num_accounts": 1000}` | (re)create + populate the dataset (optional body; defaults to 1000) |
| `/balance` | POST | `{"name":"cust1"}` | read-only: savings + checking total |
| `/deposit_checking` | POST | `{"name":"cust1","amount":50}` | add to checking |
| `/transact_savings` | POST | `{"name":"cust1","amount":-50}` | add/subtract savings (rejects overdraft) |
| `/amalgamate` | POST | `{"name1":"cust1","name2":"cust2"}` | move all of cust1's funds into cust2's checking |
| `/write_check` | POST | `{"name":"cust1","amount":50}` | debit checking; penalty if balance insufficient |
| `/send_payment` | POST | `{"from":"cust1","to":"cust2","amount":50}` | move checking funds between customers |

Accounts created by `/init_db` are named `cust1`..`custN`, each starting with a
savings and checking balance of 1000.00.

## Run locally

```bash
docker run -p 80:80 fadhilkurnia/xdn-smallbank
curl -X POST localhost/init_db -d '{"num_accounts": 100}'
curl -X POST localhost/balance -d '{"name":"cust1"}'
```

State (the SQLite database) is stored under `/app/data/`.

## Build a container image

```bash
docker build --tag=xdn-smallbank .
```

To build for multiple platforms:

```bash
docker buildx build --platform linux/amd64,linux/arm64 --tag=xdn-smallbank .
```
