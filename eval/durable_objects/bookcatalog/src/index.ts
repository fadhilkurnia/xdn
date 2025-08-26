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

  async getAllBooks(): Promise<Book[]> {
    try {
      const result = this.ctx.storage.sql.exec(`
        SELECT id, title, author FROM books
      `);

      return result.toArray().map(row => ({
        id: typeof row.id === 'number' ? row.id : undefined,
        title: String(row.title),
        author: String(row.author)
      }));
    } catch (error) {
      console.error("Error fetching books:", error);
      throw new Error("Failed to fetch books");
    }
  }

  async getBookById(id: number): Promise<Book | null> {
    try {
      const result = this.ctx.storage.sql.exec(`
        SELECT id, title, author FROM books WHERE id = ?
      `, id);
      
      const books = result.toArray();
      if (books.length === 0) {
        return null;
      }
      
      const book = books[0];
      return {
        id: typeof book.id === 'number' ? book.id : undefined,
        title: String(book.title),
        author: String(book.author)
      };
    } catch (error) {
      console.error("Error fetching book:", error);
      throw new Error("Failed to fetch book");
    }
  }

  async createBook(book: Book): Promise<Book> {
    try {
      this.ctx.storage.sql.exec(
        `INSERT INTO books (title, author) VALUES (?, ?)`,
        book.title, book.author
      );
      
      const result = this.ctx.storage.sql.exec(
        `SELECT last_insert_rowid() as id`
      );
      
      const idResult = result.one();
      const id = typeof idResult.id === 'number' ? idResult.id : undefined;
      
      return { ...book, id };
    } catch (error) {
      console.error("Error creating book:", error);
      throw new Error("Failed to create book");
    }
  }

  async updateBook(id: number, book: Book): Promise<Book | null> {
    try {
      const checkResult = this.ctx.storage.sql.exec(`
        SELECT id FROM books WHERE id = ?
      `, id);
      
      if (checkResult.toArray().length === 0) {
        return null;
      }
      
      this.ctx.storage.sql.exec(
        `UPDATE books SET title = ?, author = ? WHERE id = ?`,
        book.title, book.author, id
      );
      
      const result = this.ctx.storage.sql.exec(`
        SELECT id, title, author FROM books WHERE id = ?
      `, id);
      
      const updatedBook = result.one();
      return {
        id: typeof updatedBook.id === 'number' ? updatedBook.id : undefined,
        title: String(updatedBook.title),
        author: String(updatedBook.author)
      };
    } catch (error) {
      console.error("Error updating book:", error);
      throw new Error("Failed to update book");
    }
  }

  async deleteBook(id: number): Promise<Book | null> {
    try {
      const getResult = this.ctx.storage.sql.exec(`
        SELECT id, title, author FROM books WHERE id = ?
      `, id);
      
      const books = getResult.toArray();
      if (books.length === 0) {
        return null;
      }
      
      const book = books[0];
      const bookToReturn: Book = {
        id: typeof book.id === 'number' ? book.id : undefined,
        title: String(book.title),
        author: String(book.author)
      };
      
      this.ctx.storage.sql.exec(`
        DELETE FROM books WHERE id = ?
      `, id);
      
      return bookToReturn;
    } catch (error) {
      console.error("Error deleting book:", error);
      throw new Error("Failed to delete book");
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
      
      try {
        const pathSegments = url.pathname.split('/').filter(Boolean);
        const bookIdIndex = pathSegments.indexOf('books') + 1;
        const bookId = bookIdIndex < pathSegments.length ? 
                      parseInt(pathSegments[bookIdIndex], 10) : undefined;
        
        if (bookId === undefined || isNaN(bookId)) {
          if (request.method === 'GET') {
            const books = await stub.getAllBooks();
            return new Response(JSON.stringify(books), {
              headers: { 'Content-Type': 'application/json' }
            });
          } 
          else if (request.method === 'POST') {
            const book = await request.json() as Book;
            
            if (!book.title || !book.author) {
              return new Response(JSON.stringify({ error: "Title and author are required" }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
              });
            }
            
            const createdBook = await stub.createBook(book);
            return new Response(JSON.stringify(createdBook), {
              status: 201,
              headers: { 'Content-Type': 'application/json' }
            });
          } 
          else {
            return new Response('Method not allowed', { status: 405 });
          }
        } 
        else {
          if (request.method === 'GET') {
            const book = await stub.getBookById(bookId);
            
            if (!book) {
              return new Response(JSON.stringify({ error: "Book not found" }), {
                status: 404,
                headers: { 'Content-Type': 'application/json' }
              });
            }
            
            return new Response(JSON.stringify(book), {
              headers: { 'Content-Type': 'application/json' }
            });
          } 
          else if (request.method === 'PUT') {
            const bookData = await request.json() as Book;
            const updatedBook = await stub.updateBook(bookId, bookData);
            
            if (!updatedBook) {
              return new Response(JSON.stringify({ error: "Book not found" }), {
                status: 404,
                headers: { 'Content-Type': 'application/json' }
              });
            }
            
            return new Response(JSON.stringify(updatedBook), {
              headers: { 'Content-Type': 'application/json' }
            });
          } 
          else if (request.method === 'DELETE') {
            const deletedBook = await stub.deleteBook(bookId);
            
            if (!deletedBook) {
              return new Response(JSON.stringify({ error: "Book not found" }), {
                status: 404,
                headers: { 'Content-Type': 'application/json' }
              });
            }
            
            return new Response(JSON.stringify(deletedBook), {
              headers: { 'Content-Type': 'application/json' }
            });
          } 
          else {
            return new Response('Method not allowed', { status: 405 });
          }
        }
      } catch (error: any) {
        console.error("Error handling request:", error);
        return new Response(JSON.stringify({ 
          error: error.message || "Internal server error" 
        }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' }
        });
      }
    }
    
    return env.ASSETS.fetch(request);
  }
} satisfies ExportedHandler<Env>;