// kvnode runs one node of the cluster.
//
//	kvnode -id 1 -peers 1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"time"

	"raftkv/kv"
	"raftkv/raft"
	"raftkv/server"
)

func main() {
	id := flag.Int("id", 0, "this node's id (must appear in -peers)")
	peersFlag := flag.String("peers", "", "all nodes, e.g. 1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003")
	dataDir := flag.String("data", "", "directory for this node's state (default data-<id>)")
	flag.Parse()

	peers, err := server.ParsePeers(*peersFlag)
	if err != nil {
		log.Fatal(err)
	}
	addr, ok := peers[*id]
	if !ok {
		log.Fatalf("-id %d is not in -peers", *id)
	}
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data-%d", *id)
	}
	storage, err := raft.NewFileStorage(*dataDir)
	if err != nil {
		log.Fatal(err)
	}

	ids := make([]int, 0, len(peers))
	for p := range peers {
		ids = append(ids, p)
	}
	sort.Ints(ids)

	node, applyCh, err := raft.Start(raft.Config{
		ID: *id, Peers: ids, Transport: server.NewHTTPTransport(peers), Storage: storage,
		ElectionTimeout: server.ElectionTimeout, Heartbeat: server.Heartbeat,
		Seed: time.Now().UnixNano(),
	})
	if err != nil {
		log.Fatal(err)
	}
	store := kv.New(node, applyCh, server.RequestTimeout)
	srv := &http.Server{
		Addr:              addr,
		Handler:           (&server.Server{Node: node, Store: store, Peers: peers}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go logRoleChanges(node)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	go func() {
		<-stop
		log.Printf("node %d shutting down", *id)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	log.Printf("node %d listening on %s, data in %s", *id, addr, *dataDir)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	node.Stop()
}

// logRoleChanges prints a line whenever this node's role or term changes, so
// you can watch elections happen in the terminal.
func logRoleChanges(node *raft.Node) {
	var last raft.Status
	for range time.Tick(100 * time.Millisecond) {
		s := node.Status()
		if s.Role != last.Role || s.Term != last.Term {
			log.Printf("term %d: %s (leader=%d)", s.Term, s.Role, s.Leader)
		}
		last = s
	}
}
