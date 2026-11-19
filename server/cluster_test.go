package server

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"raftkv/kv"
	"raftkv/raft"
)

type testNode struct {
	node *raft.Node
	http *http.Server
	addr string
}

// startCluster runs three real nodes on random local ports.
func startCluster(t *testing.T) map[int]*testNode {
	t.Helper()
	listeners := map[int]net.Listener{}
	peers := map[int]string{}
	for id := 1; id <= 3; id++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id] = l
		peers[id] = l.Addr().String()
	}
	nodes := map[int]*testNode{}
	for id := 1; id <= 3; id++ {
		storage, err := raft.NewFileStorage(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		node, applyCh, err := raft.Start(raft.Config{
			ID: id, Peers: []int{1, 2, 3}, Transport: NewHTTPTransport(peers), Storage: storage,
			ElectionTimeout: ElectionTimeout, Heartbeat: Heartbeat, Seed: int64(id),
		})
		if err != nil {
			t.Fatal(err)
		}
		store := kv.New(node, applyCh, RequestTimeout)
		srv := &http.Server{Handler: (&Server{Node: node, Store: store, Peers: peers}).Handler()}
		go srv.Serve(listeners[id])
		nodes[id] = &testNode{node: node, http: srv, addr: peers[id]}
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.http.Close()
			n.node.Stop()
		}
	})
	return nodes
}

func waitForLeader(t *testing.T, nodes map[int]*testNode) int {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		for id, n := range nodes {
			if n.node.Status().Role == "leader" {
				return id
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return 0
}

func request(t *testing.T, method, addr, key, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, "http://"+addr+"/kv/"+key, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data)
}

func TestClusterOverHTTP(t *testing.T) {
	nodes := startCluster(t)
	leader := waitForLeader(t, nodes)
	follower := leader%3 + 1

	if code, _ := request(t, "PUT", nodes[leader].addr, "city", "London"); code != http.StatusNoContent {
		t.Fatalf("PUT returned %d", code)
	}
	if code, body := request(t, "GET", nodes[leader].addr, "city", ""); code != 200 || body != "London" {
		t.Fatalf("GET returned %d %q", code, body)
	}

	// A follower refuses and points to the leader.
	code, body := request(t, "GET", nodes[follower].addr, "city", "")
	if code != http.StatusMisdirectedRequest || !strings.Contains(body, nodes[leader].addr) {
		t.Fatalf("follower returned %d %s, want 421 naming %s", code, body, nodes[leader].addr)
	}

	if code, _ := request(t, "DELETE", nodes[leader].addr, "city", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE returned %d", code)
	}
	if code, _ := request(t, "GET", nodes[leader].addr, "city", ""); code != http.StatusNotFound {
		t.Fatalf("GET after DELETE returned %d, want 404", code)
	}

	for i := 0; i < 20; i++ {
		request(t, "PUT", nodes[leader].addr, fmt.Sprintf("k%d", i), fmt.Sprint(i))
	}

	// Kill the leader. The other two elect a new one that has all the data.
	nodes[leader].http.Close()
	nodes[leader].node.Stop()
	delete(nodes, leader)
	newLeader := waitForLeader(t, nodes)
	for i := 0; i < 20; i++ {
		code, body := request(t, "GET", nodes[newLeader].addr, fmt.Sprintf("k%d", i), "")
		if code != 200 || body != fmt.Sprint(i) {
			t.Fatalf("after failover k%d = %d %q", i, code, body)
		}
	}
	if code, _ := request(t, "PUT", nodes[newLeader].addr, "after", "failover"); code != http.StatusNoContent {
		t.Fatalf("PUT on new leader returned %d", code)
	}
}

func TestParsePeers(t *testing.T) {
	peers, err := ParsePeers("1=127.0.0.1:9001, 2=127.0.0.1:9002")
	if err != nil || len(peers) != 2 || peers[2] != "127.0.0.1:9002" {
		t.Fatalf("got %v, %v", peers, err)
	}
	for _, bad := range []string{"", "1:9001", "x=a", "1=a,1=b", "0=a"} {
		if _, err := ParsePeers(bad); err == nil {
			t.Errorf("ParsePeers(%q) should fail", bad)
		}
	}
}
