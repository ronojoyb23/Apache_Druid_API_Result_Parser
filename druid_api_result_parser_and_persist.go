package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type flightInfo []struct {
	Version   string    `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Event     struct {
		Origin            string `json:"origin"`
		Destination       string `json:"destination"`
		COUNTFlightNumber int    `json:"COUNT(flightNumber)"`
		FlightNumber      string `json:"flightNumber"`
	} `json:"event"`
}

//Define function to get FlightInfo objects
func getFlightInfo(body []byte) (*flightInfo, error) {
	var s = new(flightInfo)
	err := json.Unmarshal(body, &s)
	if err != nil {
		fmt.Println("Error getting Druid response JSON:", err)
	}
	return s, err
}

func main() {

	const (
		host     = "vi-analytics01.prod.tor.tpstk.ca"
		port     = 5432
		user     = "kafkaconsumer"
		password = "WVNziMGzrsheuz6cJpQCYDAz"
		dbname   = "nightsky"
	)
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("Successfully connected!")
	currentTime := time.Now()
	tableName := `flight_info_` + currentTime.Format("01_02_2006")
	fmt.Println(`Table Name: ` + tableName)
	sqlCreateStatement := `CREATE TABLE IF NOT EXISTS ` + tableName + `(flight_number VARCHAR (50) PRIMARY KEY,origin VARCHAR (50),destination VARCHAR (50),count INT)`

	_, err = db.Exec(sqlCreateStatement)
	if err != nil {
		fmt.Println(`create table statement error`)
		panic(err)
	}
	sqlInsertStatement := `INSERT into ` + tableName + ` VALUES ('AA123','AAA','BBB',100) ON CONFLICT (flight_number) DO UPDATE SET count = EXCLUDED.count`

	_, err = db.Exec(sqlInsertStatement)
	if err != nil {
		panic(err)
	}

	//Druid Queries Below

	url := "http://druid.tpstk.ca:8082/druid/v2/"
	method := "POST"
	//start_time := "\2020-08-01T00:00:00+00:00"
	//end_time := "/2020-09-09T00:00:00+00:00\"

	/*payload := strings.NewReader("{\n  \"queryType\": \"topN\",\n  \"dataSource\": \"vi-night-sky\"," +
	"\n  \"aggregations\": [\n    {\n      \"type\": \"count\",\n      \"name\": \"count\"\n    },\n" +
	"    {\n      \"fieldName\": \"count\",\n      \"fieldNames\": [\n        \"count\"\n      ],\n" +
	"      \"type\": \"count\",\n      \"name\": \"COUNT(count)\"\n    }\n  ],\n  \"granularity\": \"all\",\n" +
	"  \"postAggregations\": [],\n" +
	"  \"intervals\": \"2020-09-13T00:00:00+00:00/2020-09-14T00:00:00+00:00\",\n  \"filter\": {\n " +
	"   \"type\": \"selector\",\n    \"dimension\": \"source\",\n    \"value\": \"CMO\"\n  },\n  \"threshold\": 1,\n" +
	"  \"metric\": \"COUNT(count)\",\n  \"dimension\": \"flightNumber\"\n}")*/

	payload2 := strings.NewReader("\n  {\n  \"queryType\": \"groupBy\",\n  \"dataSource\": \"vi-night-sky\",\n  \"dimensions\": [\n    \"flightNumber\",\n    \"origin\",\n    \"destination\"\n  ],\n  \"aggregations\": [\n    {\n      \"fieldName\": \"flightNumber\",\n      \"fieldNames\": [\n        \"flightNumber\"\n      ],\n      \"type\": \"count\",\n      \"name\": \"COUNT(flightNumber)\"\n    }\n  ],\n  \"granularity\": \"all\",\n  \"postAggregations\": [],\n  \"intervals\": \"2020-09-07T00:00:00+00:00/2020-09-14T00:00:00+00:00\",\n  \"filter\": {\n    \"type\": \"selector\",\n    \"dimension\": \"source\",\n    \"value\": \"CMO\"\n  },\n  \"limitSpec\": {\n    \"type\": \"default\",\n    \"limit\": 2,\n    \"columns\": [\n      {\n        \"dimension\": \"COUNT(flightNumber)\",\n        \"direction\": \"descending\"\n      }\n    ]\n  }\n}\n")
	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload2)

	if err != nil {
		fmt.Println(err)
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	//fmt.Println(string(body))
	s, err := getFlightInfo([]byte(body))

	for _, value := range *s {
		arr := value.Event
		flightNumber := arr.FlightNumber
		origin := arr.Origin
		destination := arr.Destination
		count := strconv.Itoa(int(arr.COUNTFlightNumber))
		values := `('` + flightNumber + `','` + origin + `','` + destination + `',` + count + `)`
		sqlInsertStatement := `INSERT into ` + tableName + ` VALUES` + values + `ON CONFLICT (flight_number) DO UPDATE SET count = EXCLUDED.count`

		_, err = db.Exec(sqlInsertStatement)
		if err != nil {
			panic(err)
		}
	}
	//print database table contents after parsing json and persist to db
	sqlStatement := `SELECT * from ` + tableName
	rows, err := db.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	fmt.Println("flightNumber|Count|Origin|Destination")
	for rows.Next() {

		var flightNumber string
		var count int
		var origin string
		var destination string
		err = rows.Scan(&flightNumber, &origin, &destination, &count)
		if err != nil {
			// handle this error
			panic(err)
		}
		fmt.Println(flightNumber, `|`, count, `|`, origin, `|`, destination)
	}
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	err = db.Close()
	if err != nil {
		fmt.Println("DB connection did not close properly")
		panic(err)
	}

}
