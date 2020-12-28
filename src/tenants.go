package main

import (
	"encoding/json"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type brokerTenant struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// BrokerTenants contains pinot tenants
type BrokerTenants map[string][]*httputil.ReverseProxy

var tenants BrokerTenants = nil

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
	firstFetch := tenants == nil
	tenants = BrokerTenants{}
	for key, elements := range parsedBody {
		tenants[key] = make([]*httputil.ReverseProxy, len(elements))
		for index, tenant := range parsedBody[key] {
			tenantURLString := "http://" + strings.TrimPrefix(tenant.Host, "Broker_") + ":" + strconv.Itoa(tenant.Port)
			tenantURL, err := url.Parse(tenantURLString)
			if err != nil {
				log.WithError(err).Warn("Failed to build tenants proxys")
				if firstFetch == true {
					tenants = nil // reset only first time, if not, keep previous tenants
				}
				return
			}
			proxy := httputil.NewSingleHostReverseProxy(tenantURL)
			tenants[key][index] = proxy
			log.Debug("Found " + tenantURLString + " for '" + key + "' tenant")
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
