package database

import "database/sql"

func NewConnetion(dataset string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dataset)
	if err != nil {
		return nil, err
	}
	return db, nil
}
