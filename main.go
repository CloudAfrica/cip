package main

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
	"strings"
	"github.com/robfig/config"
	"log"
	"flag"
)

type Data struct {
	Values         []float64 `json:"values"`
	Dstypes        []string  `json:"dstypes"`
	Dsnames        []string  `json:"dsnames"`
	Time           float64   `json:"time"`
	Interval       float64   `json:"interval"`
	Host           string    `json:"host"`
	Plugin         string    `json:"plugin"`
	PluginInstance string    `json:"plugin_instance"`
	Type           string    `json:"type"`
	TypeInstance   string    `json:"type_instance"`
}

type Point struct {
	Host           string
	Plugin         string
	PluginInstance string
	Type           string
	TypeInstance   string
	Name           string
	Value          float64
}

type Influx struct {
	Name    string `json:"name"`
	Columns []string `json:"columns"`
	Points  [][]interface{} `json:"points"`
}

func (i *Influx) addPoint(a ...interface{}) {
	var e []interface{}
	for _, v := range a {
		e = append(a, v)
	}
	i.Points = append(i.Points, e)
}

func handler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	if verbose {
		log.Printf("Body %v", string(body))
	}
	var d []Data
	err = json.Unmarshal(body, &d)
	if err != nil {
		panic(err)
	}

	if verbose {
		log.Printf("Collectd %+v", d)
	}

	influx := Influx{}
	influx.Name = "events"

	influx.Columns = []string{"host", "key", "value"}

	for _, e := range d {
		for i, n := range e.Dsnames {
			if verbose {
				log.Printf("E %+v", e)
			}
			keys := []string{e.Plugin, e.PluginInstance, e.Type, e.TypeInstance, n}
			key := ""
			for i, k := range keys {
				if k != "" {
					key += k
					if i < len(keys) -1 {
						key += "."
					}
				}
			}
			if verbose {
				log.Printf("Key : %v", key)
			}
			influx.addPoint(e.Host, key, e.Values[i])
		}
	}

	b, e := json.Marshal([]Influx{influx})
	if e != nil {
		panic(e)
	}
	resp, err := http.Post(influxUrl, "application/json", strings.NewReader(string(b)))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if verbose {
		res , _ := ioutil.ReadAll(resp.Body)
		log.Printf("Influx post response %v %v", resp.Status, string(res));
	}

}

// global url for influx
var influxUrl string
var verbose bool

func main() {


	flag.BoolVar(&verbose, "verbose", false, "See lots of stuff")
	confFile := flag.String("config", "", "Config file location")
	flag.Parse()

	var (
		protocol string
		host string
		port string
		db string
		user string
		password string
		err error
		c *config.Config
	)

	c, err = config.ReadDefault(*confFile)
	if err != nil {
		log.Print("No config file found, using defaults")
		protocol = "http://"
		host = "localhost"
		port = "8086"
		db = "events"
		user = "data"
		password = "data"
	} else {

		if protocol, err = c.RawStringDefault("protocol"); err != nil {
			protocol = "http://"
		}
		if host, err = c.RawStringDefault("host"); err != nil {
			host = "localhost"
		}
		if db, err = c.RawStringDefault("db"); err != nil {
			db = "events"
		}
		if user, err = c.RawStringDefault("user"); err != nil {
			user = "data"
		}
		if user, err = c.RawStringDefault("password"); err != nil {
			user = "password"
		}
	}

	influxUrl = protocol+host+":"+port+"/db/"+db+"/series?u="+user+"&p="+password

	if verbose {
		log.Print("Starting proxy")
		log.Print("InfluxDB URL : " + influxUrl)
	}

	http.HandleFunc("/", handler)
	http.ListenAndServe(":8079", nil)
}

