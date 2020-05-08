package db

import (
	"context"

	"github.com/jackc/pgx"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

type StateHistorical struct {
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
	Hospitalized             int    `db:"hospitalized"`
	Death                    int    `db:"death"`
}

type Roach struct {
	// Db holds a sql.DB pointer that represents a pool of zero or more
	// underlying connections - safe for concurrent use by multiple
	// goroutines -, with freeing/creation of new connections all managed
	// by `sql/database` package.
	Db  *pgx.Conn
	cfg pgx.ConnConfig
}

// Config holds the configuration used for instantiating a new Roach.
type Config struct {
	// Address that locates our postgres instance
	Host string
	// Port to connect to
	Port string
	// User that has access to the database
	User string
	// Password so that the user can login
	Password string
	// Database to connect to (must have been created priorly)
	Database string
}

func New(cfg pgx.ConnConfig) (roach Roach, err error) {
	if cfg.Host == "" || cfg.User == "" ||
		cfg.Password == "" || cfg.Database == "" {
		logger.Error("All fields must be set (%s)", zap.Error(err))
		// err = errors.Errorf(
		// 	"All fields must be set (%s)",
		// 	spew.Sdump(cfg))
		return
	}

	roach.cfg = cfg

	// The first argument corresponds to the driver name that the driver
	// (in this case, `lib/pq`) used to register itself in `database/sql`.
	// The next argument specifies the parameters to be used in the connection.
	// Details about this string can be seen at https://godoc.org/github.com/lib/pq
	db, err := pgx.Connect(cfg)
	if err != nil {
		logger.Error("Couldn't open connection to postgre database (%s)", zap.Error(err))
		return
	}

	// Ping verifies if the connection to the database is alive or if a
	// new connection can be made.
	if err = db.Ping(context.Background()); err != nil {
		logger.Error("Couldn't ping postgre database (%s)", zap.Error(err))
		return
	}

	roach.Db = db
	return
}
