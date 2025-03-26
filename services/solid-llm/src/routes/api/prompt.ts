import type { APIEvent } from "@solidjs/start/server";
import { getPrompts, postPrompt } from "~/lib/db";

export async function GET() {
    console.log(`GET - /api/prompt`);
    return await getPrompts();
}

export async function POST(event: APIEvent) {
    console.log(`POST - /api/prompt`);

    const body = await new Response(event.request.body).json();

    if (!body.prompt)
        return { message: "Prompt must not be empty."}

    return await postPrompt(body.prompt)
}