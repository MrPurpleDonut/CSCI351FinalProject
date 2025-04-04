package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
)

var cities = []string{
	"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
	"Austin", "Jacksonville", "Fort Worth", "Columbus", "San Francisco", "Charlotte", "Indianapolis", "Seattle", "Denver", "Washington",
	"Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City", "Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore",
	"Milwaukee", "Albuquerque", "Tucson", "Fresno", "Sacramento", "Kansas City", "Long Beach", "Mesa", "Atlanta", "Colorado Springs",
	"Virginia Beach", "Raleigh", "Omaha", "Miami", "Oakland", "Minneapolis", "Tulsa", "Wichita", "New Orleans", "Arlington",
	"Cleveland", "Bakersfield", "Tampa", "Aurora", "Honolulu", "Anaheim", "Santa Ana", "Corpus Christi", "Riverside", "Lexington",
	"St. Louis", "Stockton", "Pittsburgh", "Saint Paul", "Cincinnati", "Anchorage", "Henderson", "Greensboro", "Plano", "Newark",
	"Toledo", "Lincoln", "Orlando", "Chula Vista", "Jersey City", "Chandler", "Fort Wayne", "Buffalo", "Durham", "St. Petersburg",
	"Irvine", "Laredo", "Madison", "Norfolk", "Lubbock", "Gilbert", "Winston-Salem", "Glendale", "Hialeah", "Garland",
	"Scottsdale", "Irving", "Chesapeake", "North Las Vegas", "Fremont", "Baton Rouge", "Richmond", "Boise", "San Bernardino", "Birmingham",
}

func main() {
	length, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}

	name := os.Args[2]

	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	rand.Seed(10) //Just something to see with

	for range length {
		city := cities[rand.Intn(len(cities))]
		negative := rand.Intn(2) == 1
		firstTwoDigits := rand.Intn(100)
		lastDigit := rand.Intn(10)

		negativeSign := "-"
		if !negative {
			negativeSign = ""
		}

		line := fmt.Sprintf("%s;%s%d.%d\n", city, negativeSign, firstTwoDigits, lastDigit)
		_, err := file.WriteString(line)
		if err != nil {
			fmt.Println("Error Writing to file:", err)
		}
	}

}
