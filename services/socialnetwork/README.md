# socialnetwork - consolidated DeathStarBench social network

A multi-container XDN service modelled on
[DeathStarBench](https://github.com/delimitrou/DeathStarBench)'s `socialNetwork`,
with all the original microservices consolidated into a single Go HTTP frontend
backed by MongoDB. It supports users, posts, follow relationships, and user/home
timelines, and ships a small browser UI at `/`.

Identifiers are assigned from a deterministic counter (not Mongo ObjectIDs) and
no document stores a wall-clock, so the replicated state is deterministic given
the request order — the service runs under XDN with `--deterministic=true`.

Tech: Go, MongoDB.

## Endpoints

| Path | Method | Body | Description |
|------|--------|------|-------------|
| `/health` | GET | — | liveness check |
| `/init_db` | POST | `{"num_users":100}` | reset + create `user1`..`userN` (optional body) |
| `/register_user` | POST | `{"username":"alice","password":"secret"}` | create a user |
| `/compose_post` | POST | `{"user_id":1,"text":"hello"}` | publish a post |
| `/follow` | POST | `{"follower_id":1,"followee_id":2}` | follow a user |
| `/unfollow` | POST | `{"follower_id":1,"followee_id":2}` | unfollow a user |
| `/user_timeline` | POST | `{"user_id":1}` | read-only: a user's own posts |
| `/home_timeline` | POST | `{"user_id":1}` | read-only: posts from everyone the user follows |

## Run locally (multi-container)

```bash
docker compose up --build
curl -X POST localhost:8080/init_db -d '{"num_users":10}'
curl -X POST localhost:8080/compose_post -d '{"user_id":1,"text":"hello xdn"}'
curl -X POST localhost:8080/follow -d '{"follower_id":2,"followee_id":1}'
curl -X POST localhost:8080/home_timeline -d '{"user_id":2}'
```

## Deploy on XDN

This is a multi-container service (a MongoDB backend + this frontend). Launch it
from the declaration file:

```bash
xdn launch socialnetwork --file=socialnetwork.yaml
```

## Build the frontend image

```bash
docker build --tag=xdn-socialnetwork .
docker buildx build --platform linux/amd64,linux/arm64 --tag=xdn-socialnetwork .
```
