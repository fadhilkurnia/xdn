import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import Database from "better-sqlite3";

const app = express();

app.use(cors());
app.use(express.json());

const db = new Database("./data/data.db", {
  readonly: false,
});

const LLM_API = "http://ollama:11434";
const PORT = 3000;

app.listen(PORT, () => {
    console.log(`Server is running. Listening to port ${PORT}`);
});

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
        const stmt = db.prepare(
            `CREATE TABLE IF NOT EXISTS prompts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                response TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`
        );
        stmt.run();
        return res.status(201).json({ message: "Database generated successfully." });
    } catch (err) {
        console.error(err);
        return res.status(401).json({ message: "Internal server error." });
    }
});

app.post('/api/db/clear', async (req, res) => {
    console.log(`POST /api/db/clear`);

    try {
        const stmt = db.prepare(`DELETE FROM prompts`);
        stmt.run();
        return res.status(201).json({ message: "Database cleared." });
    } catch (err) {
        console.error(err);
        return res.status(401).json({ message: "Internal server error." });
    } 
});

app.post('/api/prompt', async (req, res) => {
	console.log(`POST /api/prompt`);

	try {
		const prompt = req.body.prompt;
		if (!prompt) 
			return res.status(400).json({ message: "Prompt must not be empty" });

		const body = JSON.stringify({
            model: "llama3.2:1b",
            prompt: `${prompt}. Return response is a maximum of one paragraph. Don't make it longer than that.`,
            stream: false,
		});
		const result = await fetch(`${LLM_API}/api/generate`, { 
			method: "POST",
			body: body,
		});

		if (!result.ok)
			return res.status(401).json({ message: "Ollama server error" });

		const data = await result.json();
        /*
        const data = {
            response: "random response",
        }
        */

		if (data && data.response) {
            const timestamp = new Date(data.created_at).toISOString().slice(0, 19).replace('T', ' ');
            // const timestamp = new Date().toISOString().slice(0, 19).replace('T', ' ');

            const stmt = db.prepare(
                `INSERT INTO prompts (prompt, response, timestamp) VALUES (?, ?, ?)`
            );
            stmt.run(prompt, data.response, timestamp);

			return res.status(200).json({
                response: data.response
			});
		}
	} catch (err) {
		console.error(err);
        return res.status(401).json({ message: "Internal server error" });
	} 
});

app.get('/api/prompt', (req, res) => {
	console.log(`GET /api/prompt`);

    try {
        const stmt = db.prepare(
            `SELECT prompt, response FROM prompts 
            ORDER BY timestamp ASC`
        );
        const rows = stmt.all();
        return res.status(200).json({ rows: rows });
    } catch (err) {
        console.error(err);
        return res.status(401).json({ message: "Internal server error" });
    }
});
