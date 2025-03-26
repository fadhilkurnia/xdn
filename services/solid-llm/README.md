# Non-Deterministic LLM Chat Service (Alternative)
A version of the simple-llm service, but made with the SolidStart framework. The performance of this version is not as good as simple-llm.

## Technology
- SolidStart
- Ollama
- SQLite

## Deployment
### Build Docker Image
#### LLM Service
```
docker build -t solid-llm:1 .
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
xdn launch solid-llm --file=solid-llm.yaml
```
