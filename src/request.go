package main

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

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
			brokerIndex := 0
			proxy = tenants["DefaultTenant"][brokerIndex] // TODO: use tenant from request
			proxy.Transport = &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				Dial: func(network, addr string) (net.Conn, error) {
					conn, err := (&net.Dialer{
						Timeout:   30 * time.Second,
						KeepAlive: 30 * time.Second,
					}).Dial(network, addr)
					if err != nil {
						// TODO: retry
						log.WithField("addr", addr).Warn("Failed to proxy request to broker, retrying")
					}
					return conn, err
				},
				TLSHandshakeTimeout: 10 * time.Second,
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
