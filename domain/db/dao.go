package db

import (
	"context"
	"os"

	"github.com/jackc/pgx/v4"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

// Define our Database model
type CovidData struct {
	Date                     int    `db:"date"`
	State                    string `db:"state"`
	Positive                 int    `db:"positive"`
	Negative                 int    `db:"negative"`
	HospitalizedIncrease     int    `db:"hospitalized"`
	PositiveIncrease         int    `db:"postiveincrease"`
	NegativeIncrease         int    `db:"negativeincrease"`
	DeathIncrease            int    `db:"deathincrease"`
	TotalTestResults         int    `db:"totaltestresults"`
	TotalTestResultsIncrease int    `db:"totaltestresultsincrease"`
	Pending                  int    `db:"pending"`
	HospitalizedCurrently    int    `db:"hostpitalizedcurrently"`
	HospitalizedCumulative   int    `db:"hostpitalizedcumulative"`
	InIcuCurrently           int    `db:"inicucurrently"`
	InIcuCumulative          int    `db:"inicucumulative"`
	OnVentilatorCurrently    int    `db:"onvenitlatorcurrently"`
	OnVentilatorCumulative   int    `db:"onventilatorcumulative"`
	Recovered                int    `db:"recovered"`
	Hash                     string `db:"hash"`
	Hospitalized             int    `db:"hospitalized"`
	Death                    int    `db:"death"`
	LastModified             string `db:"lastmodified"`
}

// Wraps dao with Db conn
type Pilot struct {
	// Db holds a sql.DB pointer that represents a pool of zero or more
	// underlying connections - safe for concurrent use by multiple
	// goroutines -, with freeing/creation of new connections all managed
	// by `sql/database` package.
	Db *pgx.Conn
}

// News up a new DB connection
func New(databaseURL string) (pilot Pilot, err error) {
	if databaseURL == "" {
		logger.Error("DB", "Invalid dsn", zap.Error(err))
		return
	}

	db, err := pgx.Connect(context.Background(), os.Getenv(databaseURL))
	if err != nil {
		logger.Error("Couldn't open connection to postgre database (%s)", zap.Error(err))
		return
	}

	// Ping verifies if the connection to the database is alive or if a
	// new connection can be made.
	if err = db.Ping(context.Background()); err != nil {
		logger.Error("Couldn't ping postgres database (%s)", zap.Error(err))
		return
	}

	pilot.Db = db
	return
}
