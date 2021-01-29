package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/xwb1989/sqlparser"
)

type body struct {
	SQL string `json:"sql"`
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
			if proxyForTables == nil {
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
			if len(body.SQL) == 0 {
				res.WriteHeader(400)
				return
			}
			tableName, err := getTableNameFromQuery(body.SQL)
			if err != nil {
				log.WithField("sql", body.SQL).WithError(err).Error("Got invalid SQL query")
				res.WriteHeader(400)
				return
			}
			if proxyForTables[tableName] == nil {
				log.WithField("table", tableName).Error("Unable to find table broker for request")
				res.WriteHeader(503)
				return
			}
			proxy = proxyForTables[tableName]
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

func getTableNameFromQuery(query string) (string, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return "", err
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return sqlparser.GetTableName(stmt.From[0].(*sqlparser.AliasedTableExpr).Expr).String(), nil
	case *sqlparser.Insert:
		return stmt.Table.Name.String(), nil
	case *sqlparser.Update:
		return sqlparser.GetTableName(stmt.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr).String(), nil
	case *sqlparser.Delete:
		return sqlparser.GetTableName(stmt.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr).String(), nil
	}
	return "", errors.New("Failed to parse statement")
}
