// kvctl talks to the cluster. It finds the leader by itself.
//
//	kvctl put city London
//	kvctl get city
//	kvctl delete city
//	kvctl status
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

var client = &http.Client{Timeout: 5 * time.Second}

func main() {
	nodes := flag.String("nodes", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "node addresses")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: kvctl [-nodes a,b,c] put KEY VALUE | get KEY | delete KEY | status")
	}
	flag.Parse()
	args := flag.Args()
	addrs := strings.Split(*nodes, ",")
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	var err error
	switch {
	case args[0] == "status" && len(args) == 1:
		status(addrs)
		return
	case args[0] == "put" && len(args) == 3:
		_, err = do(addrs, http.MethodPut, args[1], args[2])
	case args[0] == "get" && len(args) == 2:
		var value string
		value, err = do(addrs, http.MethodGet, args[1], "")
		if err == nil {
			fmt.Println(value)
		}
	case args[0] == "delete" && len(args) == 2:
		_, err = do(addrs, http.MethodDelete, args[1], "")
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if args[0] != "get" {
		fmt.Println("OK")
	}
}

var errNotFound = errors.New("key not found")

// do tries each node. A follower answers 421 with the leader's address, which
// is tried next. During an election there may be no leader, so it retries for
// a few seconds. PUT and DELETE are safe to retry: doing them twice gives the
// same result as doing them once.
func do(addrs []string, method, key, value string) (string, error) {
	deadline := time.Now().Add(10 * time.Second)
	next := 0
	hint := ""
	var lastErr error
	for time.Now().Before(deadline) {
		addr := hint
		if addr == "" {
			addr = addrs[next%len(addrs)]
			next++
		}
		hint = ""

		req, _ := http.NewRequest(method, "http://"+addr+"/kv/"+url.PathEscape(key), strings.NewReader(value))
		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", addr, err)
			continue // node down: try the next one
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK, http.StatusNoContent:
			return string(body), nil
		case http.StatusNotFound:
			return "", errNotFound
		case http.StatusMisdirectedRequest:
			var info struct{ Leader string }
			json.Unmarshal(body, &info)
			hint = info.Leader
			if hint == "" {
				time.Sleep(200 * time.Millisecond) // election in progress
			}
			lastErr = fmt.Errorf("%s is not the leader", addr)
		default:
			lastErr = fmt.Errorf("%s: %s %s", addr, resp.Status, strings.TrimSpace(string(body)))
			time.Sleep(200 * time.Millisecond)
		}
	}
	return "", fmt.Errorf("gave up: %w", lastErr)
}

func status(addrs []string) {
	for _, addr := range addrs {
		resp, err := client.Get("http://" + addr + "/status")
		if err != nil {
			fmt.Printf("%-16s DOWN\n", addr)
			continue
		}
		var s struct {
			ID          int
			Role        string
			Term        uint64
			Leader      int
			CommitIndex uint64 `json:"commit_index"`
		}
		json.NewDecoder(resp.Body).Decode(&s)
		resp.Body.Close()
		fmt.Printf("%-16s node %d  %-9s term %-3d leader %d  commit %d\n", addr, s.ID, s.Role, s.Term, s.Leader, s.CommitIndex)
	}
}
