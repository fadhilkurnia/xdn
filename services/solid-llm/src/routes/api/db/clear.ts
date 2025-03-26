import { clearDB } from "~/lib/db";

export async function POST() {
    console.log(`POST - /api/db/clear`);
    await clearDB();
    return { message: "Database cleared (table emptied)."};
}