import { createDB } from "~/lib/db";

export async function POST() {
    console.log(`POST - /api/db/create`);
    await createDB();
    return { message: "Database created."};
}