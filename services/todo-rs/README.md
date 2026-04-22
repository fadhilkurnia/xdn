# todo-simple web service

This simple web service is specifically created as an example 
of stateful app with monotonic operations. 

This web service is a TODO-list application that store all tasks in 
the TODO-list as a Set (i.e., no duplicate tasks). All the non read-only 
operations in this app (i.e., `POST` and `DELETE`) have monotonic properties,
allowing them to be executed in any order.

Below is the endpoint of this service: 
```
GET    /view                                             // the frontend
GET    /api/todo/tasks                                   // read all tasks
POST   /api/todo/tasks  --data '{"item":"Write paper"}'  // add a new task
DELETE /api/todo/tasks  --data '{"item":"Write paper"}'  // remove a task
```

Internally, the TODO-list is representad as Grow-only Set, comprising of two 
sets: the added and removed tasks.

## Getting Started
Running the web service:
```
cargo run .
```

To run on a non-privileged port:
```
PORT=8080 cargo run .
```

To use rqlite as the datastore:
```
DB_TYPE=rqlite DB_HOST=localhost cargo run .
```
Ensure rqlite is reachable at `http://<DB_HOST>:4001` (or set `DB_HOST` to `host:port` / full URL).

To run the web service as a container:
```
docker build -t todo-simple .
docker run -p 8080:8080 todo-simple
```
