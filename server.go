package main

import (
	"cmp"
	"encoding/json"
	"fmt"
	"github.com/labstack/echo/v4"
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

	e.POST("/1brc", func(c echo.Context) error {
		averages := make(map[string]RunningAverage)
		decoder := json.NewDecoder(c.Request().Body)
		t, err := decoder.Token()
		if err != nil {
			return err
		}
		fmt.Printf("%T: %v\n", t, t)
		var m Measurement
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
		}

		t, err = decoder.Token()
		if err != nil {
			return err
		}
		fmt.Printf("%T: %v\n", t, t)

		responseData := Response1BRC{
			RacerID:  "00000000-0000-0000-0000-000000000008",
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
		return c.JSON(200, responseData)
	})

	e.Logger.Fatal(e.Start(":1323"))
}
