package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

type JsonDate time.Time

type RequestBody struct {
	Date                     int    `json: "date"`
	State                    string `json: "state"`
	Positive                 int    `json: "postive"`
	Negative                 int    `json: "negative"`
	HospitalizedIncrease     int    `json: "hostipitalizedIncrease"`
	PositiveIncrease         int    `json: "positiveIncrease"`
	NegativeIncrease         int    `json: "negativeIncrease"`
	DeathIncrease            int    `json: "deathIncrease"`
	TotalTestResults         int    `json: "totalTestResults"`
	TotalTestResultsIncrease int    `json: "totalTestResultsIncrease"`
	Pending                  int    `json: "pending, omitempty"`
	HospitalizedCurrently    int    `json: "hospitalizedCurrently"`
	HospitalizedCumulative   int    `json: "hospitalizedCumulative"`
	InIcuCurrently           int    `json: "inIcuCurrently"`
	InIcuCumulative          int    `json: "inIcuCumulative"`
	OnVentilatorCurrently    int    `json: "onVentilatorCurrently"`
	OnVentilatorCumulative   int    `json: "onVentilatorCumulative"`
	Recovered                int    `json: "recovered"`
	Hospitalized             int    `json: "Hospitalized"`
	// DateChecked              time.Time `json: "dateChecked"`
}

var myClient = &http.Client{Timeout: 10 * time.Second}

// Ingest daily historical data into Postgres
func ingestStateHistorical(url string) {
	response, err := myClient.Get(url)
	if err != nil {
		logger.Error("failed to get data", zap.Error(err))
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}

	var rb []RequestBody
	err = json.Unmarshal([]byte(body), &rb)
	if err != nil {
		logger.Error("failed to unmarshal data", zap.Error(err))
	}

	// logger.Infof("Results: %v", rb)

	// Write raw data to dump for testing
	writeURL := "raw_data.txt"
	databytes := []byte(body)
	err = ioutil.WriteFile(writeURL, databytes, 0666)
	if err != nil {
		logger.Error("Unable to write file", zap.Error(err))
	}

	// fmt.Printf("Results: %v\n", rb)
	// os.Exit(0)

	// stateInfo := db.StateHistorical{
	// 	State:                    rb.State,
	// 	Positive:                 rb.Positive,
	// 	HospitalizedIncrease:     rb.HospitalizedIncrease,
	// 	PositiveIncrease:         rb.PositiveIncrease,
	// 	NegativeIncrease:         rb.NegativeIncrease,
	// 	DeathIncrease:            rb.DeathIncrease,
	// 	TotalTestResultsIncrease: rb.TotalTestResultsIncrease,
	// 	Grade:                    rb.Grade,
	// 	Score:                    rb.Score,
	// 	Negative:                 rb.Negative,
	// 	Pending:                  rb.Pending,
	// 	HospitalizedCurrently:    rb.HospitalizedCurrently,
	// 	HospitalizedCumulative:   rb.HospitalizedCurrently,
	// 	InIcuCurrently:           rb.InIcuCurrently,
	// 	InIcuCumulative:          rb.InIcuCumulative,
	// 	OnVentilatorCurrently:    rb.OnVentilatorCurrently,
	// 	OnVentilatorCumulative:   rb.OnVentilatorCumulative,
	// 	Recovered:                rb.Recovered,
	// 	LastUpdateEt:             rb.LastUpdateEt,
	// 	CheckTimeEt:              rb.CheckTimeEt,
	// 	Hospitalized:             rb.Hospitalized,
	// 	Total:                    rb.Total,
	// 	TotalTestResults:         rb.TotalTestResults,
	// 	PosNeg:                   rb.PosNeg,
	// 	DateChecked:              rb.DateChecked,
	// }

}

// imeplement Marshaler und Unmarshalere interface
func (j *JsonDate) UnmarshalJSON(b []byte) error {

	s := strings.Trim(string(b), "\"")
	logger.Infof("string ?", s)
	// date := s.Format("2006-01-02")
	t, err := time.Parse("20060102", s)
	logger.Infof("date ?", t)
	if err != nil {
		logger.Error("Cannot parse date", zap.Error(err))
		return err
	}
	result := t.Format("2006-01-02")
	logger.Infof("date formatted ?", result)
	*j = JsonDate(t)
	return nil
}

// Maybe a Format function for printing your date
func (j JsonDate) Format(s string) string {
	t := time.Time(j)
	return t.Format(s)

}

func main() {

	newLogger := zap.NewExample()
	sugar := newLogger.Sugar()

	logger = sugar

	// States Current Data
	// currentStateURL := "https://covidtracking.com/api/v1/states/current.json"
	// States Historical Data
	dailyStateURL := "https://covidtracking.com/api/v1/states/daily.json"

	ingestStateHistorical(dailyStateURL)

}

//  --- current state data
// {
// 	"state":"AK",
// 	"positive":335,
// 	"positiveScore":1,
// 	"negativeScore":1,
// 	"negativeRegularScore":1,
// 	"commercialScore":1,
// 	"grade":"A",
// 	"score":4,
// 	"negative":11824,
// 	"pending":null,
// 	"hospitalizedCurrently":39,
// 	"hospitalizedCumulative":36,
// 	"inIcuCurrently":null,
// 	"inIcuCumulative":null,
// 	"onVentilatorCurrently":null,
// 	"onVentilatorCumulative":null,
// 	"recovered":196,
// 	"lastUpdateEt":"4/22 14:00",
// 	"checkTimeEt":"4/22 16:28",
// 	"death":9,"hospitalized":36,
// 	"total":12159,
// 	"totalTestResults":12159,
// 	"posNeg":12159,
// 	"fips":"02",
// 	"dateModified":"2020-04-22T18:00:00Z",
// 	"dateChecked":"2020-04-22T20:28:00Z",
// 	"notes":"Please stop using the \"total\" field. Use \"totalTestResults\" instead.",
// 	"hash":"309546621981856abed23495c7bf675ccfb5e915"
// }

// 	--- State historical data
// [{
// 	"date":20200424,
// 	"state":"CA",
// 	"positive":39254,
// 	"negative":454919,
// 	"pending":null,
// 	"hospitalizedCurrently":4880,
// 	"hospitalizedCumulative":null,
// 	"inIcuCurrently":1521,
// 	"inIcuCumulative":null
// 	"onVentilatorCurrently":null,
// 	"onVentilatorCumulative":null,
// 	"recovered":null,
// 	"hash":"92b2177ee03e94d3b0f5ce2ca074415cc2cce85c",
// 	"dateChecked":"2020-04-24T20:00:00Z",
// 	"death":1562,
// 	"hospitalized":null,
// 	"total":494173
// 	"totalTestResults":494173,
// 	"posNeg":494173,
// 	"fips":"06",
// 	"deathIncrease":93,
// 	"hospitalizedIncrease":0,
// 	"negativeIncrease":10191,
// 	"positiveIncrease":1885,
// 	"totalTestResultsIncrease":12076
// }
