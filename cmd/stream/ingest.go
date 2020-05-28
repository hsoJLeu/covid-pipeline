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

	"github.com/hsojleu/covid-pipeline/domain/db"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/robfig/cron/v3"

	"go.uber.org/zap"
)

var (
	logger   *zap.SugaredLogger
	conn     db.Pilot
	ctx      context.Context
	myClient = &http.Client{Timeout: 10 * time.Second}
)

const (
	//	Current state data
	currentStateURL = "https://covidtracking.com/api/v1/states/current.json"
	//	Historical state data
	dailyStateURL = "https://covidtracking.com/api/v1/states/daily.json"
	// Current US data
	currentUSURL = "https://covidtracking.com/api/v1/us/current.json"
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
	DateChecked              string `json:"dateChecked"`
}

// Ingest daily historical data into Postgres
func ingestStateHistorical(url string) error {

	response, err := myClient.Get(url)
	if err != nil {
		logger.Error("failed to get data", zap.Error(err))
		return err
	}
	defer response.Body.Close()

	// STAGE 1: INGEST - Read data
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		logger.Fatal(err)
	}

	// dump data for testing
	dumpDataToFile("ingest_statehistorical_log", body)

	// STAGE 2: Process - Unmarshll data into struct
	var rb []db.CovidData
	err = json.Unmarshal([]byte(body), &rb)
	if err != nil {
		logger.Error("failed to unmarshal data", zap.Error(err))
		return err
	}

	logger.Infof("state historical request body has: %d objects", len(rb))

	// STAGE 3: Store - Batch insert/update data into postgres
	err = conn.UpdateStateHistorical(rb)
	if err != nil {
		logger.Error("UpdateStateHistorical error: ", err)
	}

	return err
}

// Ingest daily current data into Postgres
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

	// batch insert/update data into postgres
	err = conn.UpdateStateCurrent(rb)
	if err != nil {
		logger.Error("UpdateStateCurrent error: ", err)
	}

	return err
}

func ingestUSCurrent(url string) error {

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
	dumpDataToFile("ingest_USCurrent_log", body)

	// Unmarshll data into struct
	var rb []db.CovidData
	err = json.Unmarshal([]byte(body), &rb)
	if err != nil {
		logger.Error("failed to unmarshal data", zap.Error(err))
		return err
	}

	logger.Infof("Current US request body has: %d objects", len(rb))

	// batch insert/update data into postgres
	err = conn.UpdateUSCurrent(rb)
	if err != nil {
		logger.Error("UpdateUSCurrent error: ", err)
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
		logger.Info("Connected to docker pg db", err, &conn)

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
	// ingestStateCurrent(currentStateURL)
	// ingestStateHistorical(dailyStateURL)

	// // Ingest US data
	// ingestUSCurrent(currentUSURL)

	job := cron.New()
	job.AddFunc("@every 6h", func() {
		ingestStateCurrent(currentStateURL)
		logger.Info("STATE CURRENT UPDATED")
	})
	job.AddFunc("@every 6h", func() {
		ingestStateHistorical(dailyStateURL)
		logger.Info("STATE HISTORICAL UPDATED")
	})
	job.AddFunc("@every 6h", func() {
		ingestUSCurrent(currentUSURL)
		logger.Info("US CURRENT UPDATED")
	})

	job.Start()
	select {} // Keep job alive
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
