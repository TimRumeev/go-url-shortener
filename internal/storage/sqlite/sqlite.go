package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	"ex.com/internal/storage"
	"github.com/mattn/go-sqlite3"
)

type Storage struct {
	db *sql.DB
}

func New(storagePath string) (*Storage, error) {
	const op = "storage.sqlite.New"

	db, err := sql.Open("sqlite3", storagePath)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	stmt, err := db.Prepare(`
	CREATE TABLE IF NOT EXISTS url(
		id INTEGER PRIMARY KEY,
		alias TEXT NOT NULL UNIQUE,
		url TEXT NOT NULL);
	CREATE INDEX IF NOT EXISTS idx_alias ON url(alias);
	`)

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	_, err = stmt.Exec()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) SaveUrl(urlToSave string, alias string) (int64, error) {
	const op = "storage.sqlite.SaveUrl"

	stmt, err := s.db.Prepare("INSERT INTO url(url, alias) VALUES(?, ?)")
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	res, err := stmt.Exec(urlToSave, alias)
	if err != nil {
		if sqliteErr, ok := err.(sqlite3.Error); ok && sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique {
			return 0, fmt.Errorf("%s: %w", op, storage.ERR_URL_EXISTS)
		}

		return 0, fmt.Errorf("%s: %w", op, err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("%s: failed to get last id", op)
	}

	return id, nil

}

func (s *Storage) GetUrlByAlias(alias string) (string, error) {
	const op = "storage.sqlite.GetUrlByAlias"

	stmt, err := s.db.Prepare("SELECT url FROM url WHERE alias = ?")
	if err != nil {
		return " ", fmt.Errorf("%s: %w", op, err)
	}
	var resURL string

	err = stmt.QueryRow(alias).Scan(&resURL)
	if errors.Is(err, sql.ErrNoRows) {
		return " ", storage.ERR_URL_NOT_FOUND
	}
	if err != nil {
		return " ", fmt.Errorf("%s: %w", op, err)
	}

	return resURL, nil
}

func (s *Storage) DeleteUrl(alias string) error {
	const op = "storage.sqlite.DeleteUrl"

	stmt, err := s.db.Prepare("DELETE FROM url WHERE alias = ?")
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	_, err = stmt.Exec(alias)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ERR_URL_NOT_FOUND
	}
	if err != nil {
		return fmt.Errorf("%s, %w", op, err)
	}
	return nil
}

func (s *Storage) GetAll() (string, error) {
	const op = "storage.sqlite.GetAll"
	rows, err := s.db.Query("SELECT id, url, alias FROM url")
	if err != nil {
		return " ", fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()
	var result string
	for rows.Next() {
		var id int64
		var url string
		var alias string
		if err := rows.Scan(&id, &url, &alias); err != nil {
			return " ", fmt.Errorf("%s: %w", op, err)
		}
		result += fmt.Sprintf("{id: %d, url: %s, alias: %s}, ", id, url, alias)
	}
	return result, nil
}
