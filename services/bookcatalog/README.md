# Golang - Books CRUD API

A simple REST API to CRUD books on a {mysql,postgres,sqlite} database.

## Technologies
- Golang Mux Framework
- Golang
- Gorm {Mysql,Postgres,SQlite} driver

## Launch
### Datastore: MySQL
Start the web service:
```
DB_TYPE=mysql go run main.go
```

>:warning: you need to run a docker or local MySQL at localhost:3306. The Gorm ORM framework will automatically create a table on your MySql database.

To run a docker with mysql, just run: 
```
docker run --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=books -d mysql:8
```

### Datastore: Postgres
Start the web service:
```
DB_TYPE=postgres go run main.go
```

>:warning: you need to run a docker or local Postgres at localhost:5432. The Gorm ORM framework will automatically create a table on your Postgres database.

To run a docker with postgres, just run:
```
docker run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=root -e POSTGRES_DB=books -d postgres:bookworm
```

### Datastore: SQLite
Start the web service:
```
DB_TYPE=sqlite go run main.go
```

### Datastore: rqlite
Start the web service:
```
DB_TYPE=rqlite DB_HOST=localhost go run main.go
```
Ensure that the rqlite database is running on localhost, on port 4001.

## Endpoints
#### Create
```
curl -X POST http://localhost/api/books \
 -H 'Content-Type: application/json' \
 -d '{"title": "Distributed Systems", "author": "Tanenbaum"}'
 ```
#### Read
```
curl http://localhost/api/books/{bookId} \
 -H 'Content-Type: application/json'
 ```
#### Update
```
curl -X PUT http://localhost/api/books/{bookId} \
 -H 'Content-Type: application/json' \
 -d '{"title": "Distributed Systems", "author": "Tanenbaum"}'
 ```
#### Delete
```
curl -X DELETE http://localhost/api/books/{bookId}
 ```

## Building a container image
Building a docker container, that by default uses SQLite datastore:
```
docker build --tag bookcatalog .
```
To build for multiple platforms:
```
docker buildx build --platform linux/amd64,linux/arm64 --tag=bookcatalog .
```

## Acknowledgement
This book catalog web service is extended from https://github.com/arpesam/go-book-api. We made small adjustment mainly to accommodate different datastore: `mysql`, `postgres`, and `sqlite`, also for logging & instrumentation purposes.