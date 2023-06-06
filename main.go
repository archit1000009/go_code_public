package main

import (
	"encoding/csv"
	"fmt"
	"log"

	"database/sql"
	"net/http"
	"os"

	"github.com/go-chi/chi"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	godotenv.Load()

	db := db{}

	db.make_connection()

	portString := os.Getenv("PORT")
	if portString == "" {
		log.Fatal("Port is not found in the environment")
	}

	router := chi.NewRouter()

	srv := &http.Server{
		Handler: router,
		Addr:    ":" + portString,
	}

	log.Printf("Server is starting up on %v", portString)
	err1 := srv.ListenAndServe()

	if err1 != nil {
		log.Fatal(err1)
	}

}

type db struct {
	DB sql.DB
}

// Helper function to retrieve the names of all tables in the database
func getTableNames(d *sql.DB) ([]string, error) {
	query := (`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`)
	rows, err := d.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// Helper function to retrieve the data from a table
func getDataFromTable(db *sql.DB, tableName string) ([][]string, error) {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	data := make([][]string, 0)
	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range columns {
			pointers[i] = &values[i]
		}

		err := rows.Scan(pointers...)
		if err != nil {
			return nil, err
		}
		rowData := make([]string, len(columns))
		for i, v := range values {
			if v != nil {
				rowData[i] = fmt.Sprintf("%v", v)
			}
		}

		data = append(data, rowData)
	}

	return data, nil
}

func (d *db) make_connection() {
	connStr := "user=postgres password=archit2002 dbname=go_datab host=localhost port=5432 sslmode=disable"

	// Connect to PostgreSQL database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Ping the database to check connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection established successfully")

	//Creating tables

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS borrower_details (
		borrower_id SERIAL PRIMARY KEY,
		first_name VARCHAR(100),
		last_name VARCHAR(100),
		contact_number VARCHAR(20),
		state VARCHAR(100),
		employment_status VARCHAR(50),
		credit_score INT,
		existing_debt NUMERIC
		)
	`)

	if err != nil {
		log.Fatal(err)
	}
	//fmt.Println("Table created successfully!")

	_, err = db.Exec(`
	    CREATE TABLE IF NOT EXISTS interest_rates (
		    loan_purpose VARCHAR(20) PRIMARY KEY,
		    interest_rate NUMERIC
	    )
	`)

	if err != nil {
		log.Fatal(err)
	}

	//fmt.Println("Table created successfully!")

	_, err = db.Exec(`
	    CREATE TABLE IF NOT EXISTS loan_applications (
		    loan_application_id SERIAL PRIMARY KEY,
		    borrower_id INT REFERENCES borrower_details(borrower_id),
		    loan_amount NUMERIC,
		    loan_purpose TEXT REFERENCES interest_rates(loan_purpose),
		    loan_term INT,
			repayment_schedule TEXT
		)
	`)

	if err != nil {
		log.Fatal(err)
	}

	//csv file
	// Get a list of all table names in the database
	tables, err := getTableNames(db)
	if err != nil {
		log.Fatal(err)
	}

	// Open the CSV file for writing
	file, err := os.Create("data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Iterate over each table and retrieve the data
	for _, table := range tables {
		// Retrieve the data from the current table
		data, err := getDataFromTable(db, table)
		if err != nil {
			log.Printf("Error retrieving data from table %s: %v", table, err)
			continue
		}

		// Write the data to the CSV file
		for _, record := range data {
			err := writer.Write(record)
			if err != nil {
				log.Printf("Error writing data to CSV file: %v", err)
				continue
			}
		}
	}
	var app_id string
	var borrower_id string
	err5 := db.QueryRow("SELECT loan_application_id,borrower_id FROM loan_applications").Scan(&app_id, &borrower_id)
	if err5 != nil {
		log.Fatal(err5)
	}
	fmt.Printf("app_id:%v and borrower_id:%v\n", app_id, borrower_id)
}

//fmt.Println("Table created successfully!")

// stmt2, err := db.Prepare("INSERT INTO borrower_details (borrower_id, first_name, last_name,contact_number, state, employment_status, credit_score, existing_debt) VALUES ($1, $2,$3,$4,$5,$6,$7,$8)")
// if err != nil {
// 	log.Fatal(err)
// }

// // Execute the SQL statement with the parameter values
// _, err = stmt2.Exec(2, "Ram", "jain", "12345678", "delhi", "empolyed", 650, 5000)
// if err != nil {
// 	log.Fatal(err)
// }

// stmt1, err := db.Prepare("INSERT INTO interest_rates (loan_purpose, interest_rate) VALUES ($1, $2)")
// if err != nil {
// 	log.Fatal(err)
// }

// // Execute the SQL statement with the parameter values
// _, err = stmt1.Exec("education", 6)
// if err != nil {
// 	log.Fatal(err)
// }

// stmt, err := db.Prepare("INSERT INTO loan_applications (loan_application_id, borrower_id, loan_amount, loan_purpose, loan_term, repayment_schedule) VALUES ($1, $2,$3,$4,$5,$6)")
// if err != nil {
// 	log.Fatal(err)
// }

// // Execute the SQL statement with the parameter values
// _, err = stmt.Exec(1, 2, 10000, "education", 5, "yearly")
// if err != nil {
// 	log.Fatal(err)
// }
