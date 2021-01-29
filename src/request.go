package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"

	log "github.com/sirupsen/logrus"
)

type body struct {
	Tenant string `json:"tenant"`
	SQL    string `json:"sql"`
}

// RequestHandler handle HTTP requests
func RequestHandler(pinotControllerURL string) func(http.ResponseWriter, *http.Request) {
	parsedPinotControllerURL, err := url.Parse(pinotControllerURL)
	if err != nil {
		log.WithError(err).Fatal("Unable to parse controller URL")
		os.Exit(1)
	}
	// create the controller reverse proxy
	controllerProxy := httputil.NewSingleHostReverseProxy(parsedPinotControllerURL)
	return func(res http.ResponseWriter, req *http.Request) {
		proxy := controllerProxy
		if req.URL.Path == "/query/sql" { // We want to proxy to brokers for queries
			if proxyForTenants == nil {
				log.WithError(err).Error("Unable to proxy request")
				res.WriteHeader(503)
				return
			}
			var body body
			bodyBytes, _ := ioutil.ReadAll(req.Body)
			req.Body.Close()
			req.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
			err := json.Unmarshal(bodyBytes, &body)
			if err != nil {
				res.WriteHeader(400)
				return
			}
			if len(body.Tenant) == 0 || len(body.SQL) == 0 {
				res.WriteHeader(400)
				return
			}
			if proxyForTenants[body.Tenant] == nil {
				log.WithField("tenant", body.Tenant).Error("Unable to find tenant for request")
				res.WriteHeader(503)
				return
			}
			proxy = proxyForTenants[body.Tenant]
		}
		proxy.ErrorHandler = proxyErrorHandler
		// Note that ServeHttp is non blocking & uses a go routine under the hood
		proxy.ServeHTTP(res, req)
	}
}

func proxyErrorHandler(res http.ResponseWriter, req *http.Request, err error) {
	log.WithError(err).Error("Failed to proxy request")
	res.WriteHeader(500)
}
