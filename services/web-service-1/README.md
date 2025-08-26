# An Example of Stateful Web Service

This is a simple Web Service for user directory. All the data is stored
using SQLite in the `data` directory.

How to run the Web Service:
```bash
go build
./main
```

The Web Service runs in port `:8000`. You can then use any HTTP client, 
including `curl` to issue an HTTP request. For example:
```bash
curl -X POST http://localhost:8000/api/v1/users \
   -H "Content-Type: application/json" \
   -d '{"firstname": "John", "lastname": "Doe"}'
```

REST API of this Web Service:
| Method      | Endpoint           | Description             | Example Payload | Example Request |
| ----------- | ------------------ | ----------------------- | --------------- | --------------- |
| GET         | /api/v1/users      | Get all the users       | -               | -               |
| POST        | /api/v1/users      | Create a new user       | -               | -               |
| GET         | /api/v1/users/{id} | Get a user by the ID    | -               | -               |
| PUT         | /api/v1/users/{id} | Update a user by the ID | -               | -               |
| DELETE      | /api/v1/users/{id} | Delete a user by the ID | -               | -               |

Acknowledgement: this code was modified from https://github.com/lazarospsa/rest_go_sqlite.