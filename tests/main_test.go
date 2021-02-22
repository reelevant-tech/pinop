package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"
)

func AssertEqual(t *testing.T, a interface{}, b interface{}) {
	if a == b {
		return
	}
	// debug.PrintStack()
	t.Fatalf("Received %v (type %v), expected %v (type %v)", a, reflect.TypeOf(a), b, reflect.TypeOf(b))
}

var cmd *exec.Cmd

func TestMain(m *testing.M) { // run the test
	fmt.Println("hi")
	code := m.Run()
	fmt.Println("hello", cmd.Process.Pid)
	if cmd != nil {
		// cmd.Process.Kill()
	}
	os.Exit(code)
}

func TestMainWithoutEnv(t *testing.T) {
	cmd = exec.Command("bash", "-c", "go run ../src/*.go")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}

func TestMainWitLogLevel(t *testing.T) {
	cmd = exec.Command("bash", "-c", "go run ../src/*.go")
	cmd.Env = append(os.Environ(), "PINOT_CONTROLLER_URL=http://foo", "LOG_LEVEL=invalid")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}

type controllerHandler struct {
}

func (m *controllerHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/v2/brokers/tables" {
		res.WriteHeader(200)
		// first host should fail
		fmt.Fprintf(res, "{\"foo\":[{\"host\":\"fail-lookup\",\"port\":3000},{\"host\":\"127.0.0.1\",\"port\":3002}]}")
		return
	}
	res.WriteHeader(403)
}

type brokerHandler struct {
}

func (m *brokerHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	res.WriteHeader(401)
}

func TestProxy(t *testing.T) {
	// Setup server
	go func() {
		http.ListenAndServe(":3001", &controllerHandler{})
	}()
	go func() {
		http.ListenAndServe(":3002", &brokerHandler{})
	}()

	cmd = exec.Command("bash", "-c", "go run ../src/*.go")
	cmd.Env = append(os.Environ(), "PINOT_CONTROLLER_URL=http://127.0.0.1:3001", "PORT=4000")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Start()
	if err != nil {
		t.Fatalf("process exited with err %v", err)
	}
	time.Sleep(500 * time.Millisecond) // wait http server to start

	// proxy any request
	resp, err := http.Get("http://127.0.0.1:4000/v2/brokers/tables")
	if err != nil {
		t.Fatalf("failed to proxy request to tables with: %v", err)
	}
	AssertEqual(t, resp.StatusCode, 200)
	resp, err = http.Get("http://127.0.0.1:4000/v2/foo")
	if err != nil {
		t.Fatalf("failed to proxy request to tables with: %v", err)
	}
	t.Log(out.String())
	AssertEqual(t, resp.StatusCode, 403)

	// proxy sql queries
	resp, err = http.Post("http://127.0.0.1:4000/query/sql", "application/json", bytes.NewBuffer([]byte(`{}`)))
	AssertEqual(t, resp.StatusCode, 400)
	resp, err = http.Post("http://127.0.0.1:4000/query/sql", "application/json", bytes.NewBuffer([]byte(`{"sql": "SELECT * FROM foo"}`)))
	AssertEqual(t, resp.StatusCode, 401) // broker reach
	resp, err = http.Post("http://127.0.0.1:4000/query/sql", "application/json", bytes.NewBuffer([]byte(`{"sql": "SELECT * FROM bar"}`)))
	AssertEqual(t, resp.StatusCode, 503) // broker not found
}
