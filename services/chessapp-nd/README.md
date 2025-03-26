# Non-Deterministic Chess App Service
Non-Deterministic version of the Chess App Service. This version only uses SQLite which is the only database system currently supported for a non-deterministic deployment in XDN.

## Technology
- Django
- SQLite

## Deployment
### Build Docker Image
```
docker build -t chessapp-nd:1 .
```

### Deploy Locally using Docker Compose
```
docker compose up -d
```

### Deploy on XDN 
```
xdn launch chessapp-nd --file=chessapp-nd.yaml
```
