package main

import (
	"cmp"
	"encoding/json"
	"fmt"
	"github.com/coreos/go-systemd/daemon"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"net"
	"os"
	"slices"
	"strconv"
)

type Measurement struct {
	Station     string       `json:"station"`
	Temperature RoundedFloat `json:"temperature"`
}

type RunningAverage struct {
	Value float32
	Count int64
}

type Response1BRC struct {
	RacerID  string        `json:"racerId"`
	Averages []Measurement `json:"averages"`
}

type RoundedFloat float32

func (r RoundedFloat) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatFloat(float64(r), 'f', 5, 32)), nil
}

func main() {

	e := echo.New()
	port, found := os.LookupEnv("RACER_PORT")
	if !found {
		port = "1323"
	}
	racerId, found := os.LookupEnv("RACER_ID")
	if !found {
		racerId = "00000000-0000-0000-0000-000000000008"
	}
	e.Use(middleware.Decompress())
	e.POST("/1brc", func(c echo.Context) error {
		e.Logger.Print("starting 1brc")
		averages := make(map[string]RunningAverage)
		decoder := json.NewDecoder(c.Request().Body)
		_, err := decoder.Token()
		if err != nil {
			return err
		}
		//fmt.Printf("%T: %v\n", t, t)
		var m Measurement
		rows := 0
		for decoder.More() {
			err = decoder.Decode(&m)

			if err != nil {
				return err
			}
			avg := averages[m.Station]
			avg.Count += 1
			avg.Value += (float32(m.Temperature) - avg.Value) / float32(avg.Count)
			//fmt.Printf("%s (%d): %.5f\n", m.Station, avg.Count, avg.Value)
			averages[m.Station] = avg
			rows++
			if rows%1000000 == 0 {
				e.Logger.Printf("[1brc] processed %d rows", rows)
			}
		}

		_, err = decoder.Token()
		if err != nil {
			return err
		}
		//fmt.Printf("%T: %v\n", t, t)

		responseData := Response1BRC{
			RacerID:  racerId,
			Averages: nil,
		}
		for station, avg := range averages {
			responseData.Averages = append(responseData.Averages, Measurement{
				Station:     station,
				Temperature: RoundedFloat(avg.Value),
			})
		}
		slices.SortFunc(responseData.Averages, func(a, b Measurement) int {
			return cmp.Compare(a.Station, b.Station)
		})
		e.Logger.Printf("Finished 1brc %d rows", rows)
		return c.JSON(200, responseData)
	})

	l, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		e.Logger.Fatal(err)
	}
	e.Listener = l
	daemon.SdNotify(false, daemon.SdNotifyReady)
	e.Logger.Fatal(e.Start(""))
}
