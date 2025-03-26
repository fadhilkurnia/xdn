# Non-Deterministic LLM Chat Service
Basic chat app made with only pure HTML, CSS, and JS with an Express.js back-end.

## Technology
- Express
- Ollama
- SQLite

## Deployment
### Build Docker Image
#### LLM Service
```
docker build -t simple-llm:1 .
```
#### Ollama Image
There are multiple different versions of the simple-ollama image according to the LLM model used. Any of them works.
```
docker build . -t simple-ollama:1 -f ./ollama/Dockerfile.deepseek-r1:1.5b
docker build . -t simple-ollama:1 -f ./ollama/Dockerfile.llama3.2:1b
```

### Deploy Locally using Docker Compose
```
docker compose up -d
docker compose down
```

### Deploy on XDN 
```
xdn launch simple-llm --file=simple-llm.yaml
```
