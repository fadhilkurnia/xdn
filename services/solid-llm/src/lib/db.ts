import Database from "better-sqlite3";
import { Chat } from "./db.type";
import { query } from "@solidjs/router";

const db = new Database("./data/data.db", {
  readonly: false,
});
const LLM_API = "http://ollama:11434";

export async function createDB() {
  "use server";
  console.log(`createDB()`);

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
  } catch (err) {
    console.error(err);
  }
}

export async function clearDB() {
  "use server";
  console.log(`clearDB()`);

  try {
    const stmt = db.prepare(`TRUNCATE TABLE prompts`);
    stmt.run();
  } catch (err) {
    console.error(err);
  }
}

export const getPrompts = query(async () => {
  "use server";
  console.log(`getPrompts()`);

  const stmt = db.prepare(
    `SELECT prompt, response FROM prompts 
    ORDER BY timestamp ASC`
  );
  const rows = stmt.all();
  return rows as Chat[];
}, "prompts");

export const postPrompt = query(async (prompt: string) => {
  "use server";
  console.log(`postPrompt() - ${prompt}`);

  try {
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
      throw new Error("Ollama server error.");

    const data = await result.json();

    if (data && data.response) {
      const timestamp = new Date(data.created_at)
        .toISOString()
        .slice(0, 19)
        .replace("T", " ");

        insertPrompt(prompt, data.response,  timestamp)
        return data.response;
    }

  } catch (err) {
    console.error(err);
    return new Response(JSON.stringify({
      message: `${err}`,
    }), {
      status: 500,
      headers: {
        "Content-Type": "application/json",
      },
    });
  }
}, "postPrompt");

export async function insertPrompt(
  prompt: string,
  response: string,
  timestamp: string
) {
  "use server";
  console.log(`insertPrompt()`);
  console.log(`prompt = ${prompt}`);
  console.log(`response = ${response}`);
  console.log(`timestamp = ${timestamp}`);

  try {
    const stmt = db.prepare(
      `INSERT INTO prompts (prompt, response, timestamp) VALUES (?, ?, ?)`
    );
    stmt.run(prompt, response, timestamp);
  } catch (err) {
    console.error(err);
  }
}

createDB();
