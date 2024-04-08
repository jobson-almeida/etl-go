package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"

	"gihub.com/jobson-almeida/etl-go/database"
	_ "github.com/mattn/go-sqlite3"
)

type Record struct {
	ID               int    `csv:"Index" db:"id"`
	CustomerId       string `csv:"Customer Id" db:"customer_id"`
	FirstName        string `csv:"First Name" db:"first_name"`
	LastName         string `csv:"Last Name" db:"last_name"`
	Company          string `csv:"Company" db:"company"`
	City             string `csv:"City" db:"city"`
	Country          string `csv:"Country" db:"country"`
	Phone1           string `csv:"Phone 1" db:"phone_1"`
	Phone2           string `csv:"Phone 2" db:"phone_2"`
	Email            string `csv:"Email" db:"email"`
	SubscriptionDate string `csv:"Subscription Date" db:"subscription_date"`
	Website          string `csv:"Website" db:"website"`
}

func extract(dataset string) ([][]string, error) {
	file, err := os.Open(dataset)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		return nil, err
	}

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func transform(db *sql.DB, rows [][]string) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS records (
				 id INTEGER NOT NULL PRIMARY KEY,
				 customer_id TEXT NOT NULL,
				 first_name TEXT NOT NULL,
				 last_name TEXT NOT NULL,
				 company TEXT NOT NULL,
				 city TEXT NOT NULL,
				 country TEXT NOT NULL,
				 phone_1 TEXT NOT NULL,
				 phone_2 TEXT NOT NULL,
				 email TEXT NOT NULL,
				 subscription_date TEXT NOT NULL,
				 website TEXT NOT NULL
				)
		 `)
	if err != nil {
		return err
	}

	records := make([]*Record, 0, len(rows))
	for _, row := range rows {
		id, err := strconv.Atoi(row[0])
		if err != nil {
			return err
		}

		records = append(records, &Record{
			ID:               id,
			CustomerId:       row[1],
			FirstName:        row[2],
			LastName:         row[3],
			Company:          row[4],
			City:             row[5],
			Country:          row[6],
			Phone1:           row[7],
			Phone2:           row[8],
			Email:            row[9],
			SubscriptionDate: row[10],
			Website:          row[11],
		})
	}

	for _, record := range records {
		stmt, err := db.Prepare(`INSERT INTO records (
			id, 
			customer_id,
			first_name, 
			last_name, 
			company, city,
			country, phone_1,
			phone_2, 
			email,
			subscription_date,
			website
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		_, err = stmt.Exec(
			record.ID,
			record.CustomerId,
			record.FirstName,
			record.LastName,
			record.Company,
			record.City,
			record.Country,
			record.Phone1,
			record.Phone2,
			record.Email,
			record.SubscriptionDate,
			record.Website,
		)

		if err != nil {
			return err
		}
	}
	return nil
}

func load(db *sql.DB) error {
	res, err := db.Query("SELECT * FROM records")
	if err != nil {
		return err
	}

	if !res.Next() {
		return err
	}
	for res.Next() {
		var id int
		var customer_id string
		var first_name string
		var last_name string
		var company string
		var city string
		var country string
		var phone_1 string
		var phone_2 string
		var email string
		var subscription_date string
		var website string

		if err := res.Scan(
			&id,
			&customer_id,
			&first_name,
			&last_name,
			&company,
			&city,
			&country,
			&phone_1,
			&phone_2,
			&email,
			&subscription_date,
			&website,
		); err != nil {
			return err
		}
		fmt.Println("Record:",
			id,
			customer_id,
			first_name,
			last_name,
			company,
			city,
			country,
			phone_1,
			phone_2,
			email,
			subscription_date,
			website)
	}

	if err := res.Err(); err != nil {
		return err
	}
	return nil
}

// ETL
func main() {
	db, err := database.NewConnetion("customers.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// EXTRACT
	rows, err := extract("customers.csv")
	if err != nil {
		log.Fatalln(err)
	}
	// TRANSFORM
	err = transform(db, rows)
	if err != nil {
		log.Fatalln(err)
	}
	// LOAD
	err = load(db)
	if err != nil {
		log.Fatalln(err)
	}
}
