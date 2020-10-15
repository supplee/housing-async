package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func main() {
	// A slice of sample websites
	c := make(chan string, 5)

	urls := []string {
		"https://sfbay.craigslist.org/search/apa",
		// "http://www.craigslist.org",
	}

	for _, url := range urls {
		go checkUrl(url, c)
	}

	for range urls {
		fmt.Print(<-c)
	}
}

// checks and prints a message if a website is up or down
func checkUrl(url string, c chan string) {
	resp, err := http.Get(url)
	if err != nil {
		c <- url+" is down !!!"
		return
	}
	defer resp.Body.Close()
	content, err := ioutil.ReadAll(resp.Body)

	c <- string(content)
}

