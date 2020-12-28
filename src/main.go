package main

import (
	"net/http"
	"os"
	"time"

	env "github.com/Netflix/go-env"
	log "github.com/sirupsen/logrus"
)

type environment struct {
	Port     string `env:"PORT,default=3000"`
	LogLevel string `env:"LOG_LEVEL,default=info"`
	Pinot    struct {
		ControllerURL      string `env:"PINOT_CONTROLLER_URL,required=true"`
		TenantRefreshDelay int    `env:"PINOT_TENANT_REFRESH_DELAY,default=60000"`
	}
}

func main() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	log.SetOutput(os.Stdout)

	// Get env
	var environment environment
	_, err := env.UnmarshalFromEnviron(&environment)
	if err != nil {
		log.WithError(err).Fatal("Failed to parse env")
		os.Exit(1)
	}

	// Set level from env
	logLevel, err := log.ParseLevel(environment.LogLevel)
	if err != nil {
		log.WithError(err).Fatal("Failed to parse log level")
		os.Exit(1)
	}
	log.SetLevel(logLevel)

	// Start refreshing tenants
	ScheduleTenantRefresh(
		environment.Pinot.ControllerURL,
		time.Duration(int64(environment.Pinot.TenantRefreshDelay)*1000000)) // ms to ns

	// Start server
	http.HandleFunc("/", RequestHandler(environment.Pinot.ControllerURL))
	log.Info("Proxy starting on " + environment.Port)
	log.Fatal(http.ListenAndServe(":"+environment.Port, nil))
}
