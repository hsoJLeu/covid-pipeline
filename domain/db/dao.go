package db

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
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
	DateChecked              string `db:"datechecked"`
}

// Wraps dao with Db conn
type Pilot struct {
	// Db holds a sql.DB pointer that represents a pool of zero or more
	// underlying connections - safe for concurrent use by multiple
	// goroutines -, with freeing/creation of new connections all managed
	// by `sql/database` package.
	Db *pgxpool.Pool
}

// News up a new DB connection
func New(databaseURL string) (pilot Pilot, err error) {
	if databaseURL == "" {
		logger.Error("DB", "Invalid dsn", zap.Error(err))
		return
	}

	db, err := pgxpool.Connect(context.Background(), os.Getenv(databaseURL))
	if err != nil {
		logger.Error("Couldn't open connection to postgre database (%s)", zap.Error(err))
		return
	}

	// Ping verifies if the connection to the database is alive or if a
	// new connection can be made.
	// if err = db.Ping(context.Background()); err != nil {
	// 	logger.Error("Couldn't ping postgres database (%s)", zap.Error(err))
	// 	return
	// }

	pilot.Db = db
	return
}

func (p *Pilot) UpdateStateHistorical(rb []CovidData) error {
	if rb == nil {
		return errors.New("Request body cannot be nil")
	}

	batch := &pgx.Batch{}
	numInserts := len(rb)

	sql :=
		`insert into statehistorical (
			date, state, positive, negative, pending,
			hospitalizedCurrently, hospitalizedCumulative, inIcuCurrently, inIcuCumulative,
			onVentilatorCurrently, onVentilatorCumulative,
			recovered, death, hospitalized, totaltestresults,
			hospitalizedincrease, deathincrease, negativeIncrease, positiveIncrease, totaltestresultsincrease,
			hash)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
			ON CONFLICT (hash) DO NOTHING;`

	for i := 0; i < numInserts; i++ {
		ref := &rb[i]
		batch.Queue(sql,
			ref.Date, ref.State, ref.Positive, ref.Negative, ref.Pending,
			ref.HospitalizedCurrently, ref.HospitalizedCumulative, ref.InIcuCurrently, ref.InIcuCumulative,
			ref.OnVentilatorCurrently, ref.OnVentilatorCumulative,
			ref.Recovered, ref.Death, ref.Hospitalized, ref.TotalTestResults,
			ref.HospitalizedIncrease, ref.DeathIncrease, ref.NegativeIncrease, ref.PositiveIncrease, ref.TotalTestResultsIncrease,
			ref.Hash)
	}

	br := p.Db.SendBatch(context.Background(), batch)
	err := br.Close()
	if err != nil {
		logger.Fatalf("Unable to close batch request: %s", err)
		return err
	}

	return err
}

func (p *Pilot) UpdateStateCurrent(rb []CovidData) error {
	if rb == nil {
		return errors.New("Request body cannot be nil")
	}

	batch := &pgx.Batch{}
	numInserts := len(rb)

	sql := `insert into statecurrent (
    			state, positive, negative, recovered, death,
    			hospitalized, totaltestresults, datechecked, hash)
			VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (state)
			DO UPDATE SET
				positive=$2,
				negative=$3,
				recovered=$4, death=$5,
    			hospitalized=$6, totaltestresults=$7, datechecked=$8, hash=$9;
			`
	for i := 0; i < numInserts; i++ {
		ref := &rb[i]
		batch.Queue(sql,
			ref.State, ref.Positive, ref.Negative, ref.Recovered, ref.Death,
			ref.Hospitalized, ref.TotalTestResults, ref.DateChecked, ref.Hash)
	}

	br := p.Db.SendBatch(context.Background(), batch)
	exec, err := br.Exec()
	res := exec.RowsAffected()
	fmt.Printf("STATE CURRENT Rows affected: %d\n", res)

	err = br.Close()
	if err != nil {
		logger.Fatal("Unable to close batch request", err)
	}

	return err
}

func (p *Pilot) UpdateUSCurrent(rb []CovidData) error {
	if rb == nil {
		return errors.New("Request body cannot be nil")
	}

	sql := `insert into uscurrent (
    			state, positive, negative, recovered, death,
    			hospitalized, totaltestresults, lastmodified, hash)
			VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (state)
			DO UPDATE SET
				positive=$2,
				negative=$3,
				recovered=$4, death=$5,
    			hospitalized=$6, totaltestresults=$7, lastmodified=$8, hash=$9;
			`
	ref := &rb[0]
	result, err := p.Db.Exec(context.Background(), sql, ref.State, ref.Positive, ref.Negative, ref.Recovered, ref.Death,
		ref.Hospitalized, ref.TotalTestResults, ref.LastModified, ref.Hash)
	if err != nil {
		logger.Error("Could not insert into USCURRENT: ", zap.Error(err))
	}
	res := result.RowsAffected()
	fmt.Printf("US CURRENT Rows affected: %d\n", res)

	// for i := 0; i <= numInserts; i++ {
	// 	batch.Queue(sql,
	// 		)
	// }

	// br := p.Db.SendBatch(context.Background(), batch)
	// err := br.Close()
	// if err != nil {
	// 	logger.Fatal("Unable to close batch request", err)
	// }

	return err
}
