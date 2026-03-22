import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { MongoClient } from "mongodb";

const app = express();
const mongoClient = new MongoClient(process.env.MONGO_URI ?? "mongodb://mongodb:27017", {
  serverSelectionTimeoutMS: 2000,
});
const LLM_API = process.env.LLM_API ?? "http://ollama:11434";
const MODEL_NAME = process.env.MODEL_NAME ?? "llama3.2:1b";
const MONGO_DB_NAME = process.env.MONGO_DB_NAME ?? "simple-llm";
const PROMPTS_COLLECTION_NAME = "prompts";
const MAX_MONGO_CONNECT_ATTEMPTS = 10;
const MONGO_RETRY_DELAY_MS = 1000;
const parsedPort = Number.parseInt(process.env.PORT ?? "3000", 10);
const PORT = Number.isNaN(parsedPort) ? 3000 : parsedPort;
let database;
let promptsCollection;

app.use(cors());
app.use(express.json());

async function sleep(ms) {
    await new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

async function ensurePromptsCollection() {
    if (!database) {
        throw new Error("MongoDB is not initialized.");
    }

    const collectionExists = await database.listCollections(
        { name: PROMPTS_COLLECTION_NAME },
        { nameOnly: true }
    ).hasNext();

    if (!collectionExists) {
        await database.createCollection(PROMPTS_COLLECTION_NAME);
    }

    promptsCollection = database.collection(PROMPTS_COLLECTION_NAME);
    await promptsCollection.createIndex({ createdAt: 1, _id: 1 });
}

async function initializeDatabase() {
    if (!database) {
        let lastError;

        for (let attempt = 1; attempt <= MAX_MONGO_CONNECT_ATTEMPTS; attempt += 1) {
            try {
                await mongoClient.connect();
                database = mongoClient.db(MONGO_DB_NAME);
                break;
            } catch (err) {
                lastError = err;
                console.error(
                    `Failed to connect to MongoDB (attempt ${attempt}/${MAX_MONGO_CONNECT_ATTEMPTS}).`,
                    err
                );

                if (attempt < MAX_MONGO_CONNECT_ATTEMPTS) {
                    await sleep(MONGO_RETRY_DELAY_MS);
                }
            }
        }

        if (!database) {
            throw lastError;
        }
    }

    await ensurePromptsCollection();
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => {
    console.log("GET /");
    res.sendFile(path.join(__dirname, 'public/index.html'));
});

app.post('/api/db/create', async (req, res) => {
    console.log(`POST /api/db/create`);

    try {
        await initializeDatabase();
        return res.status(200).json({ message: "Database is ready." });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ message: "Internal server error." });
    }
});

app.post('/api/db/clear', async (req, res) => {
    console.log(`POST /api/db/clear`);

    try {
        await promptsCollection.deleteMany({});
        return res.status(200).json({ message: "Database cleared." });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ message: "Internal server error." });
    } 
});

app.post('/api/prompt', async (req, res) => {
	console.log(`POST /api/prompt`);

	try {
		const prompt = typeof req.body.prompt === "string" ? req.body.prompt : "";
		if (!prompt.trim())
			return res.status(400).json({ message: "Prompt must not be empty" });

		const body = JSON.stringify({
            model: MODEL_NAME,
            prompt: `${prompt.trim()}. Return response is a maximum of one paragraph. Don't make it longer than that.`,
            stream: false,
		});
		const result = await fetch(`${LLM_API}/api/generate`, { 
			method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
			body: body,
		});

		if (!result.ok)
			return res.status(502).json({ message: "Ollama server error" });

		const data = await result.json();
		if (!data || typeof data.response !== "string" || !data.response.trim())
            return res.status(502).json({ message: "Ollama response was invalid" });

        const createdAt = new Date(data.created_at ?? Date.now());
        const safeCreatedAt = Number.isNaN(createdAt.getTime())
            ? new Date()
            : createdAt;
        await promptsCollection.insertOne({
            prompt: prompt.trim(),
            response: data.response,
            createdAt: safeCreatedAt,
        });

		return res.status(200).json({
            response: data.response
		});
	} catch (err) {
		console.error(err);
        return res.status(500).json({ message: "Internal server error" });
	} 
});

app.get('/api/prompt', async (req, res) => {
	console.log(`GET /api/prompt`);

    try {
        const rows = await promptsCollection.find(
            {},
            {
                projection: {
                    _id: 0,
                    prompt: 1,
                    response: 1,
                },
            }
        ).sort({ createdAt: 1, _id: 1 }).toArray();
        return res.status(200).json({ rows: rows });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ message: "Internal server error" });
    }
});

try {
    await initializeDatabase();
} catch (err) {
    console.error("Failed to initialize database.", err);
    process.exit(1);
}

app.listen(PORT, () => {
    console.log(`Server is running. Listening to port ${PORT}`);
});
