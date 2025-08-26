import { DurableObject } from "cloudflare:workers";

export class BookCatalog extends DurableObject {

    constructor(controller, env) {
        super(controller, env);
        this.storage = controller.storage;
        this.env = env;

        // Create table if it doesn't exist
        this.storage.sql.exec(`
            CREATE TABLE IF NOT EXISTS books (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                author TEXT NOT NULL
            );
        `);
    }

    async getAllBooks() {
        // Return all books
        return this.storage.sql.exec(`
            SELECT id, title, author FROM books;
        `).toArray();
    }

    async createOneBook(title, author) {
        // Insert a new book
        this.storage.sql.exec(
            `INSERT INTO books (title, author) VALUES (?, ?);`,
            title,
            author
        );
        // Retrieve the newly inserted ID
        let row = this.storage.sql.exec(`
            SELECT last_insert_rowid() as id;
        `).one();
        return {
            id: row.id,
            title: title,
            author: author
        };
    }

    async getOneBook(bookId) {
        //: Retrieve a single book by ID
        let row = this.storage.sql.exec(`
            SELECT id, title, author
            FROM books
            WHERE id = ?;
        `, bookId).toArray();

        if (row && row[0]) {
            return {
                id: row.id,
                title: row.title,
                author: row.author
            };
        }
        // If no book found, return null
        return null;
    }

    async updateOneBook(bookId, title, author) {
        //: Update a single book by ID
        this.storage.sql.exec(`
            UPDATE books
            SET title = ?, author = ?
            WHERE id = ?;
        `, title, author, bookId);

        // Return the updated row (or null if not found)
        let row = this.storage.sql.exec(`
            SELECT id, title, author
            FROM books
            WHERE id = ?;
        `, bookId).one();

        if (row) {
            return {
                id: row.id,
                title: row.title,
                author: row.author
            };
        }
        return null;
    }

    async deleteOneBook(bookId) {
        // Delete a single book by ID
        this.storage.sql.exec(`
            DELETE FROM books
            WHERE id = ?;
        `, bookId);

        // For simplicity, just return a success indicator
        return { success: true };
    }
}

export default {
    async fetch(req, env) {
        const startTime = performance.now();
        
        try {
            // Get a stub to our Durable Object
            const id = env.catalog.idFromName("catalog");
            const stub = env.catalog.get(id);

            console.log("Receiving request: " + req.url + " serving DO instance " + id);

            let url = new URL(req.url);
            let path = url.pathname.slice(1).split('/');

            //
            // GET /api/books
            //
            if (path.length === 2 && path[0] === 'api' && path[1] === 'books' && req.method === "GET") {
                let results = await stub.getAllBooks();
                return new Response(JSON.stringify(results), {
                    headers: { "Content-Type": "application/json" }
                });
            }

            //
            // GET /api/books/:bookId
            //
            if (path.length === 3 && path[0] === 'api' && path[1] === 'books' && req.method === "GET") {
                let bookId = path[2];
                let result = await stub.getOneBook(bookId);

                if (!result) {
                    return new Response("Not found", { status: 404 });
                }

                return new Response(JSON.stringify(result), {
                    headers: { "Content-Type": "application/json" }
                });
            }

            //
            // POST /api/books
            //
            if (path.length === 2 && path[0] === 'api' && path[1] === 'books' && req.method === "POST") {
                // parse the JSON body and create a new book
                const { title, author } = await req.json();
                const result = await stub.createOneBook(title, author);

                return new Response(JSON.stringify(result), {
                    headers: { "Content-Type": "application/json" }
                });
            }

            //
            // PUT /api/books/:bookId
            //
            if (path.length === 3 && path[0] === 'api' && path[1] === 'books' && req.method === "PUT") {
                // parse the JSON body and update the book
                const bookId = path[2];
                const { title, author } = await req.json();
                const result = await stub.updateOneBook(bookId, title, author);

                if (!result) {
                    return new Response("Not found", { status: 404 });
                }

                return new Response(JSON.stringify(result), {
                    headers: { "Content-Type": "application/json" }
                });
            }

            //
            // DELETE /api/books/:bookId
            //
            if (path.length === 3 && path[0] === 'api' && path[1] === 'books' && req.method === "DELETE") {
                // delete the book
                const bookId = path[2];
                const result = await stub.deleteOneBook(bookId);

                return new Response(JSON.stringify(result), {
                    headers: { "Content-Type": "application/json" }
                });
            }

            //
            // Handle unhandled endpoints
            //
            return new Response("404 Not Found", { status: 404 });
        } finally {
            const endTime = performance.now();
            const elapsed = (endTime - startTime).toFixed(2); // in ms

            // Log the method + path + elapsed time
            const url = new URL(req.url);
            console.log(`[Request] ${req.method} ${url.pathname} - Elapsed: ${elapsed} ms`);
        }
    }
};

