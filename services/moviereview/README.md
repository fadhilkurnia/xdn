# moviereview - consolidated DeathStarBench movie-review service

A multi-container XDN service modelled on
[DeathStarBench](https://github.com/delimitrou/DeathStarBench)'s
`media-microservices`, with the microservices consolidated into a single Go HTTP
frontend backed by MongoDB. It supports movies, users, and ratings/reviews, and
ships a small browser UI at `/`.

Identifiers are assigned from a deterministic counter (not Mongo ObjectIDs) and
no document stores a wall-clock, so the replicated state is deterministic given
the request order — the service runs under XDN with `--deterministic=true`.

Tech: Go, MongoDB.

## Endpoints

| Path | Method | Body | Description |
|------|--------|------|-------------|
| `/health` | GET | — | liveness check |
| `/init_db` | POST | `{"num_movies":50,"num_users":100}` | reset + create movies and users (optional body) |
| `/register_user` | POST | `{"username":"alice","password":"secret"}` | create a user |
| `/compose_review` | POST | `{"user_id":1,"movie_id":1,"rating":5,"text":"great"}` | post a review (rating 1-5) |
| `/movie_reviews` | POST | `{"movie_id":1}` | read-only: reviews for a movie |
| `/user_reviews` | POST | `{"user_id":1}` | read-only: reviews by a user |
| `/movie_info` | POST | `{"movie_id":1}` | read-only: title, review count, average rating |

## Run locally (multi-container)

```bash
docker compose up --build
curl -X POST localhost:8080/init_db -d '{"num_movies":5,"num_users":10}'
curl -X POST localhost:8080/compose_review -d '{"user_id":1,"movie_id":1,"rating":5,"text":"great"}'
curl -X POST localhost:8080/movie_info -d '{"movie_id":1}'
```

## Deploy on XDN

This is a multi-container service (a MongoDB backend + this frontend). Launch it
from the declaration file:

```bash
xdn launch moviereview --file=moviereview.yaml
```

## Build the frontend image

```bash
docker build --tag=xdn-moviereview .
docker buildx build --platform linux/amd64,linux/arm64 --tag=xdn-moviereview .
```
