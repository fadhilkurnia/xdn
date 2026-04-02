package raftnode

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/arpesam/go-book-api/src/models"
	"github.com/hashicorp/raft"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type BookFSM struct {
	db     *gorm.DB
	dbPath string
}

func NewBookFSM(db *gorm.DB, dbPath string) *BookFSM {
	return &BookFSM{db: db, dbPath: dbPath}
}

// Apply implements raft.FSM — this is where FSYNC #2 happens (SQLite journal commit).
func (f *BookFSM) Apply(l *raft.Log) interface{} {
	cmd, err := Decode(l.Data)
	if err != nil {
		log.Printf("fsm: failed to decode command: %v", err)
		return err
	}

	switch cmd.Type {
	case CmdCreateBook:
		book := &models.Book{
			Title:  cmd.Title,
			Author: cmd.Author,
		}
		if result := f.db.Create(book); result.Error != nil {
			return result.Error
		}
		return book

	case CmdUpdateBook:
		var book models.Book
		if result := f.db.First(&book, cmd.BookID); result.Error != nil {
			return result.Error
		}
		if cmd.Title != "" {
			book.Title = cmd.Title
		}
		if cmd.Author != "" {
			book.Author = cmd.Author
		}
		if result := f.db.Save(&book); result.Error != nil {
			return result.Error
		}
		return &book

	case CmdDeleteBook:
		if result := f.db.Delete(&models.Book{}, cmd.BookID); result.Error != nil {
			return result.Error
		}
		return nil

	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

// Snapshot returns a consistent snapshot of the SQLite database using VACUUM INTO.
func (f *BookFSM) Snapshot() (raft.FSMSnapshot, error) {
	snapPath := f.dbPath + ".snapshot"
	result := f.db.Exec(fmt.Sprintf("VACUUM INTO '%s'", snapPath))
	if result.Error != nil {
		return nil, fmt.Errorf("fsm: VACUUM INTO failed: %w", result.Error)
	}
	return &BookSnapshot{path: snapPath}, nil
}

// Restore replaces the SQLite database with the snapshot contents.
func (f *BookFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	// Close the current DB connection
	sqlDB, err := f.db.DB()
	if err != nil {
		return fmt.Errorf("fsm: failed to get sql.DB: %w", err)
	}
	sqlDB.Close()

	// Overwrite the SQLite file
	out, err := os.Create(f.dbPath)
	if err != nil {
		return fmt.Errorf("fsm: failed to create db file: %w", err)
	}
	if _, err := io.Copy(out, rc); err != nil {
		out.Close()
		return fmt.Errorf("fsm: failed to write db file: %w", err)
	}
	out.Close()

	// Reopen the database
	db, err := gorm.Open(sqlite.Open("file:"+f.dbPath), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
	})
	if err != nil {
		return fmt.Errorf("fsm: failed to reopen db: %w", err)
	}
	f.db = db

	// Update the global models DB reference
	models.Init(db)

	return nil
}

type BookSnapshot struct {
	path string
}

// Persist writes the snapshot file to the raft snapshot sink.
func (s *BookSnapshot) Persist(sink raft.SnapshotSink) error {
	f, err := os.Open(s.path)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("snapshot: failed to open snapshot file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(sink, f); err != nil {
		sink.Cancel()
		return fmt.Errorf("snapshot: failed to write to sink: %w", err)
	}

	return sink.Close()
}

// Release removes the temporary snapshot file.
func (s *BookSnapshot) Release() {
	os.Remove(s.path)
}
