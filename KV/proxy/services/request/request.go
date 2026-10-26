package services

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"

	"github.com/crizah/KV/proxy/services/health"
	"github.com/crizah/KV/proxy/services/router"

	"github.com/daulet/tokenizers"
)

// Config holds the per-model settings the proxy needs to tokenise and hash
// exactly like the SGLang servers do.
type Config struct {
	Model         Model
	PageSize      int    // must equal the servers' --page-size
	TokenizerPath string // path to the model's tokenizer.json (the full fast tokenizer file)
}

type RequestService struct {
	model       Model
	input       string
	pageSize    int
	routeTable  *router.RouteTable
	tokeniser   *sharedTokenizer
	healthTable *health.HealthTable
}

type Model string

const (
	QWEN306B Model = "Qwen3-0.6B"
)

// sharedTokenizer wraps a tokenizer so concurrent requests can share one
// instance; Encode calls are serialized per tokenizer.
type sharedTokenizer struct {
	mx sync.Mutex
	tk *tokenizers.Tokenizer
}

// tokenizers are expensive to load, so they are created once per path and
// shared by every request
var tokenizerCache = struct {
	mx sync.Mutex
	m  map[string]*sharedTokenizer
}{m: make(map[string]*sharedTokenizer)}

func getTokenizer(path string) (*sharedTokenizer, error) {
	tokenizerCache.mx.Lock()
	defer tokenizerCache.mx.Unlock()
	if st, ok := tokenizerCache.m[path]; ok {
		return st, nil
	}
	tk, err := tokenizers.FromFile(path)
	if err != nil {
		return nil, err
	}
	st := &sharedTokenizer{tk: tk}
	tokenizerCache.m[path] = st
	return st, nil
}

func NewRequestService(cfg Config, input string, table *router.RouteTable, n *health.HealthTable) (*RequestService, error) {
	if cfg.PageSize <= 0 {
		return nil, fmt.Errorf("pageSize must be > 0, got %d", cfg.PageSize)
	}
	tk, err := getTokenizer(cfg.TokenizerPath)
	if err != nil {
		return nil, err
	}
	return &RequestService{model: cfg.Model, input: input, pageSize: cfg.PageSize, routeTable: table, tokeniser: tk, healthTable: n}, nil
}

func (req *RequestService) HandleReq(ctx context.Context) error {
	// tokenise her
	encoded, err := req.tokenise()
	if err != nil {
		return fmt.Errorf("error encoding %s", err.Error())
	}

	// hash her in a chain until we hit a cache miss
	N, err := req.hasher(ctx, encoded, nil)
	if err != nil {
		return fmt.Errorf("handleReq error %s", err.Error())
	}
	// at this point N should never be null right?
	node := req.router(ctx, N)
	if node == nil {
		// we didnt find any heakthy nodes lol
		// TODO: add a retry with a buffer here
		return fmt.Errorf("no healthy nodes lol")
	}
	// now, based on this node info, we send the actual req to the nodes
	return nil
}

type routerRes struct {
	nodeD    string   // the node to send the rteq to
	nodeP    string   // the node to pull the req from. if set, we send with the special headers
	metaData []string // the metadata needed to send with a pull req (the matched hash)
}

func (req *RequestService) router(ctx context.Context, R *hasherRes) *routerRes {
	// read all the healthy nodes we can use
	healthyNodes := req.healthTable.GetHealthyNodes(ctx)
	if len(healthyNodes.SortedNodes) == 0 {
		// whelp
		return nil
	}
	if R.exists {
		// cache exists
		var unhealthyNodes []string
		// iterate in sorted order
		var healthyN []string
		for n := range R.nodes {
			// for every node that we do want, check if its healthy
			_, ok := healthyNodes.NodesMap[n]
			if !ok {
				// the node we want is not healthy, track it as unhealthy node
				unhealthyNodes = append(unhealthyNodes, n)
			} else {
				// otherwise, we store it as heakthy node
				healthyN = append(healthyN, n)
			}
		}

		// sort unhealthyNodes and healthN based on healthyNodes.SortedNodes
		sortByRef(unhealthyNodes, healthyNodes.SortedNodes)
		sortByRef(healthyN, healthyNodes.SortedNodes)

		if len(healthyN) == 0 {
			// we didnt find any that we wanted, so we try to choose from unhealthy
			if len(unhealthyNodes) == 0 {
				// whelp, we just choose a random one from healthyNodea
				return &routerRes{nodeD: healthyNodes.SortedNodes[0].IP, nodeP: "", metaData: []string{}} // TODO: get metadata we need
			}
			return &routerRes{nodeD: healthyNodes.SortedNodes[0].IP, nodeP: unhealthyNodes[0], metaData: []string{}} // TODO: get metadata we need
		}

		// otherwise, we just get the healthyN we did find
		return &routerRes{nodeD: healthyN[0], nodeP: "", metaData: []string{}} // TODO: get metadata we need
	} else {
		return &routerRes{nodeD: healthyNodes.SortedNodes[0].IP, nodeP: "", metaData: []string{}} // TODO: get whatever metadata we need here
	}
}

func getRank(b []health.NodeInfo, val string) int {
	for i, v := range b {
		if v.IP == val {
			return i
		}
	}
	// this will never happen
	return -1
}

func sortByRef(a []string, b []health.NodeInfo) {
	// sorts a based on ref from b
	slices.SortFunc(a, func(x, y string) int {
		rankX := getRank(b, x)
		rankY := getRank(b, y)
		return cmp.Compare(rankX, rankY)
	})
}

func (req *RequestService) tokenise() ([]uint32, error) {
	// the tokenizer is shared across requests, so Encode calls are serialized
	req.tokeniser.mx.Lock()
	defer req.tokeniser.mx.Unlock()
	encoded, _, err := req.tokeniser.tk.EncodeErr(req.input, true) // SGLang sets this as true, we want to replicate
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

type hasherRes struct {
	exists bool
	nodes  map[string]struct{}
	// hashes and matched are kept ONLY for the transfer-vs-recompute threshold
	// experiment. they are NOT sent to the node.
	hashes  []int64
	matched int
}

func (req *RequestService) hasher(ctx context.Context, tokens []uint32, priorDigest []byte) (*hasherRes, error) {
	// int(SHA256(parent_digest + tokens)[:16], 16) reinterpreted as int64
	// NOTE: pageSize comes from config and must be the same on both server and proxy
	//
	// return false if we didnt find a cache hit

	numPages := len(tokens) / req.pageSize
	hashes := make([]int64, 0, numPages)

	digest := priorDigest
	flag := false
	N := make(map[string]struct{})

	for p := 0; p < numPages; p++ {
		// TODO: use the count/pages of blocks matched for the caliberation of
		// "if below n pages matched, dont bother with the transfer, recomputation is faster"
		// also, in a future design we could store all intermediary matched hashes as
		// well and on a failure node, we could take into account these smaller hashes
		// (if they exist on other nodes) while caculating the best node to send this
		// request to

		page := tokens[p*req.pageSize : (p+1)*req.pageSize]
		digest = hashOnePage(page, digest)

		// takes the first 8 bytes of a SHA-256 digest, interprets them as a big-endian uint64, and reinterprets as int64.
		bigInd := int64(binary.BigEndian.Uint64(digest[:8]))

		//search if this is a miss right here, if it is, we wont find anything further so stop
		// only continue if we get a cache hit

		nodes := req.routeTable.ReadCache(ctx, bigInd)

		if len(nodes) == 0 {
			// we didnt find any servers that have this guy for this digest
			if !flag {
				// we never even found one
				return &hasherRes{exists: false}, nil
			}
			// otherwise, we had found one, so we return the previosuly set nodes
			return &hasherRes{exists: true, nodes: N, hashes: hashes, matched: len(hashes)}, nil
		}

		// otherwise, cache hit, update vars
		flag = true
		N = nodes
		hashes = append(hashes, bigInd)
	}

	return &hasherRes{exists: true, nodes: N, hashes: hashes, matched: len(hashes)}, nil
}

func hashOnePage(tokens []uint32, priorDigest []byte) []byte {
	h := sha256.New()
	if len(priorDigest) == 32 {
		h.Write(priorDigest)
	}
	// Tokens as little-endian uint32 — matches the C++ raw-bytes path.
	buf := make([]byte, len(tokens)*4)
	for i, t := range tokens {
		binary.LittleEndian.PutUint32(buf[i*4:], t)
	}
	h.Write(buf)
	return h.Sum(nil)
}
