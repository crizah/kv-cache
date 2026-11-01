package node

// Node is one SGLang server as the proxy sees it.
// All addresses are built from ip + ports at config load time; nothing is hardcoded elsewhere.
type Node struct {
	ID           string // e.g. "A", "B"
	AgentURL     string // http://ip:port — proxy forwards requests here; agent sits in front of SGLang
	KVEventsAddr string // ip:port        — ZMQ KV events PUB socket (SGLang publishes, proxy subscribes)
	LoadAddr     string // ip:port        — ZMQ load publisher socket (SGLang publishes, proxy subscribes)
}
