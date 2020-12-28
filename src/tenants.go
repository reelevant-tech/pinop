package main

import (
	"encoding/json"
	"net/http"
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
type BrokerTenants map[string][]string

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
	tenants = BrokerTenants{}
	for key, elements := range parsedBody {
		tenants[key] = make([]string, len(elements))
		for index, tenant := range parsedBody[key] {
			tenantURLString := strings.TrimPrefix(tenant.Host, "Broker_") + ":" + strconv.Itoa(tenant.Port)
			tenants[key][index] = tenantURLString
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
