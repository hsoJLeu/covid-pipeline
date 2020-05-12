package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"time"

	"github.com/go-co-op/gocron"
	"github.com/hsojleu/covid-pipeline/domain/db"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/jackc/pgx/v4"

	"go.uber.org/zap"
)

var (
	logger   *zap.SugaredLogger
	conn     db.Pilot
	ctx      context.Context
	myClient = &http.Client{Timeout: 10 * time.Second}
)

const (
	//	Current state Data
	currentStateURL = "https://covidtracking.com/api/v1/states/current.json"
	//	Historical state Data
	dailyStateURL = "https://covidtracking.com/api/v1/states/daily.json"
)

type RequestBody struct {
	Date                     int    `json:"date"`
	State                    string `json:"state"`
	Positive                 int    `json:"positive"`
	Negative                 int    `json:"negative"`
	HospitalizedIncrease     int    `json:"hospitalizedIncrease"`
	PositiveIncrease         int    `json:"positiveIncrease"`
	NegativeIncrease         int    `json:"negativeIncrease"`
	DeathIncrease            int    `json:"deathIncrease"`
	TotalTestResults         int    `json:"totalTestResults"`
	TotalTestResultsIncrease int    `json:"totalTestResultsIncrease"`
	Pending                  int    `json:"pending,omitempty"`
	HospitalizedCurrently    int    `json:"hospitalizedCurrently"`
	HospitalizedCumulative   int    `json:"hospitalizedCumulative"`
	InIcuCurrently           int    `json:"inIcuCurrently"`
	InIcuCumulative          int    `json:"inIcuCumulative"`
	OnVentilatorCurrently    int    `json:"onVentilatorCurrently"`
	OnVentilatorCumulative   int    `json:"onVentilatorCumulative"`
	Recovered                int    `json:"recovered"`
	Hash                     string `json:"hash"`
	Hospitalized             int    `json:"hospitalized"`
	Death                    int    `json:"death"`
	LastModified             string `json:"lastModified"`
}

// Ingest daily historical data into Postgres
func ingestStateHistorical(url string) error {

	response, err := myClient.Get(url)
	if err != nil {
		logger.Error("failed to get data", zap.Error(err))
		return err
	}
	defer response.Body.Close()

	// Read data
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		logger.Fatal(err)
	}

	// dump data for testing
	dumpDataToFile("ingest_statehistorical_log", body)

	// Unmarshll data into struct
	var rb []db.CovidData
	err = json.Unmarshal([]byte(body), &rb)
	if err != nil {
		logger.Error("failed to unmarshal data", zap.Error(err))
		return err
	}

	logger.Infof("state historical request body has: %d objects", len(rb))

	// batch insert data into postgres
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

	br := conn.Db.SendBatch(context.Background(), batch)
	err = br.Close()
	if err != nil {
		logger.Fatal("Unable to close batch request", err)
	}

	return err
}

func ingestStateCurrent(url string) error {

	response, err := myClient.Get(url)
	if err != nil {
		logger.Error("failed to get data", zap.Error(err))
		return err
	}
	defer response.Body.Close()

	// Read data
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		logger.Fatal(err)
	}

	// dump data for testing
	dumpDataToFile("ingest_statecurrent_log", body)

	// Unmarshll data into struct
	var rb []db.CovidData
	err = json.Unmarshal([]byte(body), &rb)
	if err != nil {
		logger.Error("failed to unmarshal data", zap.Error(err))
		return err
	}

	logger.Infof("Current state request body has: %d objects", len(rb))

	// batch insert data into postgres
	batch := &pgx.Batch{}
	numInserts := len(rb)

	sql := `insert into statecurrent (
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
	for i := 0; i < numInserts; i++ {
		ref := &rb[i]
		batch.Queue(sql,
			ref.State, ref.Positive, ref.Negative, ref.Recovered, ref.Death,
			ref.Hospitalized, ref.TotalTestResults, ref.LastModified, ref.Hash)
	}

	br := conn.Db.SendBatch(ctx, batch)
	err = br.Close()
	if err != nil {
		logger.Fatal("Unable to close batch request", err)
	}

	return err
}

func init() {
	logger = zap.NewExample().Sugar()
	defer logger.Sync()

	err := errors.New("Init error")
	switch os.Args[1] {
	case "PG_CONFIG":
		conn, err = db.New("PG_CONFIG")
		logger.Info("Connected to RDS db", err, &conn)

	case "RDS_CONFIG":
		conn, err = db.New("RDS_CONFIG")
		logger.Info("Connected to RDS db", err, &conn)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

}

func main() {
	ctx = context.Background()

	// Ingest state data
	ingestStateCurrent(currentStateURL)
	ingestStateHistorical(dailyStateURL)

	defer conn.Db.Close(context.Background())

	// Fetch data every 3 hours
	job := gocron.NewScheduler(time.Local)
	job.Every(2).Minutes().Do(main)
	// job.Every(3).Hours().Do(main)

	// NextRun gets the next running time
	_, time := job.NextRun()
	logger.Infof("Next job will run at: %s", time)

	<-job.Start()
}

func dumpDataToFile(path string, body []byte) error {
	if path == "" {
		logger.Error("No path name specified")
		return errors.New("No path name specified to dump data")
	}
	writeURL := path
	databytes := []byte(body)

	err := ioutil.WriteFile(writeURL, databytes, 0644)
	if err != nil {
		logger.Error("Unable to write file", zap.Error(err))
		return err
	}

	return err
}
