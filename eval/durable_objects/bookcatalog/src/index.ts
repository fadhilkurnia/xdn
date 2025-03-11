import { DurableObject } from "cloudflare:workers";
export class BookCatalogDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.initializeDB();
  }

  private initializeDB() {
    try {
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS books (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          title TEXT NOT NULL,
          author TEXT NOT NULL
        )
      `);
    } catch (error) {
      console.error("Error initializing database:", error);
    }
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const pathSegments = url.pathname.split('/').filter(Boolean);
    
    if (pathSegments.includes('books')) {
      const bookId = pathSegments[pathSegments.indexOf('books') + 1];
      
      if (!bookId) {
        switch (request.method) {
          case 'GET':
            return this.getAllBooks();
          case 'POST':
            return this.createBook(request);
          default:
            return new Response('Method not allowed', { status: 405 });
        }
      } 

      else {
        const id = parseInt(bookId, 10);
        if (isNaN(id)) {
          return new Response('Invalid book ID', { status: 400 });
        }
        
        switch (request.method) {
          case 'GET':
            return this.getBookById(id);
          case 'PUT':
            return this.updateBook(id, request);
          case 'DELETE':
            return this.deleteBook(id);
          default:
            return new Response('Method not allowed', { status: 405 });
        }
      }
    }
    
    return new Response('Not found', { status: 404 });
  }

  async getAllBooks(): Promise<Response> {
    try {
      const result = this.ctx.storage.sql.exec(`
        SELECT id, title, author FROM books
      `);
      
      const books = result.toArray();
      return new Response(JSON.stringify(books), {
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (error) {
      console.error("Error fetching books:", error);
      return new Response(JSON.stringify({ error: "Failed to fetch books" }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  async getBookById(id: number): Promise<Response> {
    try {
      const result = this.ctx.storage.sql.exec(`
        SELECT id, title, author FROM books WHERE id = ?
      `, id);
      
      const books = result.toArray();
      if (books.length === 0) {
        return new Response(JSON.stringify({ error: "Book not found" }), {
          status: 404,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      return new Response(JSON.stringify(books[0]), {
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (error) {
      console.error("Error fetching book:", error);
      return new Response(JSON.stringify({ error: "Failed to fetch book" }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  async createBook(request: Request): Promise<Response> {
    try {
      const book = await request.json() as Book;
      
      if (!book.title || !book.author) {
        return new Response(JSON.stringify({ error: "Title and author are required" }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      this.ctx.storage.sql.exec(
        `INSERT INTO books (title, author) VALUES (?, ?)`,
        book.title, book.author
      );
      
      const result = this.ctx.storage.sql.exec(
        `SELECT last_insert_rowid() as id`
      );
      
      const id = result.one().id;
      return new Response(JSON.stringify({ ...book, id }), {
        status: 201,
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (error) {
      console.error("Error creating book:", error);
      return new Response(JSON.stringify({ error: "Failed to create book" }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  async updateBook(id: number, request: Request): Promise<Response> {
    try {
      const checkResult = this.ctx.storage.sql.exec(`
        SELECT id FROM books WHERE id = ?
      `, id);
      
      if (checkResult.toArray().length === 0) {
        return new Response(JSON.stringify({ error: "Book not found" }), {
          status: 404,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      const book = await request.json() as Book;
      
      this.ctx.storage.sql.exec(
        `UPDATE books SET title = ?, author = ? WHERE id = ?`,
        book.title, book.author, id
      );
      
      const result = this.ctx.storage.sql.exec(`
        SELECT id, title, author FROM books WHERE id = ?
      `, id);
      
      return new Response(JSON.stringify(result.one()), {
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (error) {
      console.error("Error updating book:", error);
      return new Response(JSON.stringify({ error: "Failed to update book" }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  async deleteBook(id: number): Promise<Response> {
    try {
      const getResult = this.ctx.storage.sql.exec(`
        SELECT id, title, author FROM books WHERE id = ?
      `, id);
      
      const books = getResult.toArray();
      if (books.length === 0) {
        return new Response(JSON.stringify({ error: "Book not found" }), {
          status: 404,
          headers: { 'Content-Type': 'application/json' }
        });
      }
      
      this.ctx.storage.sql.exec(`
        DELETE FROM books WHERE id = ?
      `, id);
      
      return new Response(JSON.stringify(books[0]), {
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (error) {
      console.error("Error deleting book:", error);
      return new Response(JSON.stringify({ error: "Failed to delete book" }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    
    if (url.pathname === '/') {
      return Response.redirect(`${url.origin}/view`, 301);
    }
    
    if (url.pathname === '/view') {
      return env.ASSETS.fetch(request);
    }
    
    if (url.pathname.startsWith('/api/books')) {
      const id = env.BOOK_CATALOG.idFromName('default');
      const stub = env.BOOK_CATALOG.get(id);
      return stub.fetch(request);
    }
    
    return env.ASSETS.fetch(request);
  }
} satisfies ExportedHandler<Env>;