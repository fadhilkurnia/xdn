# Nondeterministic Book Catalog CRUD Service

This service is exactly the same as the Book Catalog Service, execpt that
this service has non-deterministic operations, including recording timestamp
of book creation, update, and deletion. The changes are at the 
[`src/models/books.go`](./src/models/books.go) file.

Thus, this service is not ideal for replicated state machine which 
requires deterministic operation.

Acknowledgement: the service is adapted from 
[arpesam](https://github.com/arpesam/golang-books-api), with additional 
bugfixes and simple frontend by fadhilkurnia.