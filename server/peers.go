package server

import (
	"fmt"
	"strconv"
	"strings"
)

// ParsePeers reads "1=127.0.0.1:9001,2=127.0.0.1:9002" into a map.
func ParsePeers(s string) (map[int]string, error) {
	peers := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idText, addr, ok := strings.Cut(part, "=")
		if !ok || addr == "" {
			return nil, fmt.Errorf("peer %q should look like 1=127.0.0.1:9001", part)
		}
		id, err := strconv.Atoi(idText)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("peer id %q must be a positive number", idText)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("peer id %d listed twice", id)
		}
		peers[id] = addr
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("no peers given")
	}
	return peers, nil
}
