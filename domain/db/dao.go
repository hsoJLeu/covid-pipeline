package db

import (
	"database/sql"
	"fmt"
	"time"
)

type StateHistorical struct {
	Date                     int
	State                    string
	Positive                 int
	Negative                 int
	HospitalizedIncrease     int
	PositiveIncrease         int
	NegativeIncrease         int
	DeathIncrease            int
	TotalTestResults         int
	TotalTestResultsIncrease int
	Pending                  int
	HospitalizedCurrently    int
	HospitalizedCumulative   int
	InIcuCurrently           int
	InIcuCumulative          int
	OnVentilatorCumulative   int
	Recovered                int
	Hospitalized             int
}

unc init() {
    var err error

    dsn := MysqlConnectionString("parseTime=true")
    tablePrefix := "qcommerce"

    gorm.DefaultTableNameHandler = func(db *gorm.DB, defaultTableName string) string {
        return fmt.Sprintf("%v_%v", tablePrefix, defaultTableName)
    }

    Manager, err = gorm.Open("mysql", dsn)
    if err != nil {
        log.Fatal(err)
    }

    if err := Manager.DB().Ping(); err != nil {
        log.Fatal(err)
    }
}


func New() {
	// conn, err := pgx.Connect(context.Background(), os.Getenv("postgres://user:pass@localhost/db"))
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
	// 	os.Exit(1)
	// }

	db, err := sql.Open("postgres", "postgres://user:pass@localhost/covid")

	if err != nil {
		panic(err)
	}

	if err = db.Ping(); err != nil {
		panic(err)
	}

	fmt.Println("Successfully connect to db")
}
