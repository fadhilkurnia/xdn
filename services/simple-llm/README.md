# Non-Deterministic LLM Chat Service
Basic chat app made with only pure HTML, CSS, and JS with an Express.js back-end.

## Technology
- Express
- Ollama
- MongoDB

The server connects to MongoDB on startup and stores prompt/response history in the `prompts` collection.

## Deployment
### Build Docker Image
#### LLM Service
```
docker build -t simple-llm:1 .
```
#### Ollama Image
There are multiple different versions of the `simple-ollama` image according to the LLM model used. The backend model must match the Ollama image you build.
```
docker build . -t simple-ollama:1 -f ./ollama/Dockerfile.deepseek-r1:1.5b
docker build . -t simple-ollama:1 -f ./ollama/Dockerfile.llama3.2:1b
```

Set `MODEL_NAME` to the same model tag used in the Ollama image:
- `MODEL_NAME=llama3.2:1b`
- `MODEL_NAME=deepseek-r1:1.5b`

### Deploy Locally using Docker Compose
The provided Compose file starts Ollama, MongoDB, and the Express server together. It persists MongoDB data in a named volume and defaults `MODEL_NAME` to `llama3.2:1b`.
```
docker compose up -d
docker compose down
```

### Deploy on XDN 
```
xdn launch simple-llm --file=simple-llm.yaml
```
