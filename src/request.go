package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"

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
			if tenants == nil {
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
			brokerIndex := 0
			brokerList := tenants[body.Tenant]
			if brokerList == nil {
				log.WithField("tenant", body.Tenant).Error("Unable to find tenant for request")
				res.WriteHeader(503)
				return
			}
			proxy = &httputil.ReverseProxy{
				Director: func(r *http.Request) {
					r.URL.Scheme = "http"
					r.URL.Host = "127.0.0.1" // placeholder, will be override
					r.URL.Path = "/query/sql"
				},
				Transport: &http.Transport{
					Dial: func(network, _ string) (net.Conn, error) {
						var conn net.Conn
						var err error
						for brokerIndex < len(brokerList) { // Retry with every urls we have if connection failed
							addr := brokerList[brokerIndex]
							conn, err = (&net.Dialer{
								Timeout:   30 * time.Second,
								KeepAlive: 30 * time.Second,
							}).Dial(network, addr)
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
		proxy.ErrorHandler = proxyErrorHandler
		// Note that ServeHttp is non blocking & uses a go routine under the hood
		proxy.ServeHTTP(res, req)
	}
}

func proxyErrorHandler(res http.ResponseWriter, req *http.Request, err error) {
	log.WithError(err).Error("Failed to proxy request")
	res.WriteHeader(500)
}
