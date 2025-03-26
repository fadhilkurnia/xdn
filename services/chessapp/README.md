# Deterministic Chess App Service
Simple Chess App using REST and long polling for user data access. This service supports the use PostgreSQL, MySQL, and MariaDB. Docker compose and XDN service files for all the deployments are provided in the `deployment-scripts/` directory.

## Technology
- Django
- PostgreSQL
- MySQL
- MariaDB

## Deployment
### Build Docker Image
```
docker build -t chessapp:1 .
```

### Deploy Locally using Docker Compose
#### PostgreSQL
```
docker compose -f ./deployment-scripts/docker-compose.postgresql.yml up -d
```
#### MySQL
```
docker compose -f ./deployment-scripts/docker-compose.mysql.yml up -d
```
#### MariaDB
```
docker compose -f ./deployment-scripts/docker-compose.mariadb.yml up -d
```

### Deploy on XDN 
#### PostgreSQL
```
xdn launch chessapp --file=chessapp.postgresql.yaml
```
#### MySQL
```
xdn launch chessapp --file=chessapp.mysql.yaml
```
#### MariaDB
```
xdn launch chessapp --file=chessapp.mariadb.yaml
```
