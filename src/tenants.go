package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type brokerTenant struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type proxyTenants map[string]*httputil.ReverseProxy

var proxyForTenants proxyTenants = nil

var httpClient = &http.Client{Timeout: 10 * time.Second}

func buildTenantsFromController(pinotControllerURL string) {
	resp, err := httpClient.Get(pinotControllerURL + "/v2/brokers/tenants")
	if err != nil {
		log.WithError(err).Warn("Failed to refresh tenants")
		return
	}
	defer resp.Body.Close()
	var parsedBody map[string][]brokerTenant
	err = json.NewDecoder(resp.Body).Decode(&parsedBody)
	if err != nil {
		log.WithError(err).Warn("Failed to refresh tenants")
		return
	}
	proxyForTenants = proxyTenants{}
	for key, brokerList := range parsedBody {
		// Create list of dialers to have keep-alive
		dialers := make([]*net.Dialer, 0)
		for range brokerList {
			dialers = append(dialers, (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}))
		}
		// Create proxy
		proxyForTenants[key] = &httputil.ReverseProxy{
			Director: func(r *http.Request) {
				r.URL.Scheme = "http"
				r.URL.Host = "127.0.0.1" // placeholder, will be override
				r.URL.Path = "/query/sql"
			},
			Transport: &http.Transport{
				Dial: func(network, _ string) (net.Conn, error) {
					var conn net.Conn
					var err error
					brokerIndex := 0
					for brokerIndex < len(brokerList) { // Retry with every urls we have if connection failed
						addr := strings.TrimPrefix(brokerList[brokerIndex].Host, "Broker_") + ":" + strconv.Itoa(brokerList[brokerIndex].Port)
						log.Debug("Request sent to " + addr)
						conn, err = dialers[brokerIndex].Dial(network, addr)
						brokerIndex++
						if err == nil {
							break
						}
						logMsg := log.WithField("addr", addr).WithError(err)
						if brokerIndex < len(brokerList) {
							logMsg.Warn("Failed to proxy request to broker, retrying")
						} else {
							logMsg.Warn("Failed to proxy request to broker after retries, send 500")
						}
					}
					return conn, err
				},
			},
		}
	}
	log.Info("Tenants successfuly updated")
}

// ScheduleTenantRefresh refresh tenants list every `delay`
func ScheduleTenantRefresh(pinotControllerURL string, delay time.Duration) chan bool {
	stop := make(chan bool)

	go func() {
		for {
			buildTenantsFromController(pinotControllerURL)
			select {
			case <-time.After(delay):
			case <-stop:
				return
			}
		}
	}()

	return stop
}
