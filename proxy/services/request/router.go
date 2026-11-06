package request

import (
	"context"

	"github.com/crizah/KV/proxy/services/health"
	"github.com/crizah/KV/proxy/services/node"
)

type Router interface {
	router(ctx context.Context, R *hasherRes) *node.RouterRes
}

type KvRouter struct {
	req *RequestService
}

type RRRouter struct {
	req *RequestService
}

type LLRouter struct {
	req *RequestService
}

type CacheRouter struct {
	req                *RequestService
	alpha, beta, gamma float64 // non-negative, sum to 1, Alpha > Beta
	a, b, c            float64 // TTFT model coefficients (calibrated)
	tMax               float64 // p99 TTFT from calibration window, seconds
}

func NewKvRouter(req *RequestService) *KvRouter {
	return &KvRouter{req: req}
}

func NewRRRouter(req *RequestService) *RRRouter {
	return &RRRouter{req: req}
}

func NewCacheRouter(req *RequestService) *CacheRouter {
	// todo: hardcode values for the constants here
	return &CacheRouter{req: req}
}
func NewLLRouter(req *RequestService) *LLRouter {
	return &LLRouter{req: req}
}

func (r *KvRouter) router(ctx context.Context, R *hasherRes) *node.RouterRes {
	// read all the alive nodes we can use
	available := r.req.healthTable.GetHealthyNodes(ctx)
	if len(available.SortedNodes) == 0 {
		// whelp, what do u want me to do here lol
		// TODO: do smth :sob
		return nil
	}

	// the first healthy node in the sorted array is our default destination.
	// loaded nodes are valid pull sources but we dont route fresh work to them.
	var dest node.Node
	for _, n := range available.SortedNodes {
		if n.Status == health.StatusHealthy {
			dest = n.Node
			break
		}
	}
	if dest.ID == "" {
		// every alive node is loaded, nothing worth routing to
		// TODO: add a retry with a buffer here
		return nil
	}

	if !R.exists {
		// cache miss everywhere: send to the healthiest node, no pull
		return &node.RouterRes{NodeD: dest, NodeP: node.Node{}, MetaData: []string{}} // TODO: get metadata we need
	}

	// cache hit somewhere. sort the holders by their health status
	var healthyHolders []node.Node
	var loadedHolders []node.Node
	for holderID := range R.nodes {
		status, ok := available.NodesMap[holderID]
		if !ok {
			// unhealthy: not in the map. cant route to it AND cant pull from
			// it either, a dead node serves no bytes
			continue
		}
		nd, ok := r.req.nodeMap[holderID]
		if !ok {
			continue
		}
		if status == health.StatusHealthy {
			healthyHolders = append(healthyHolders, nd)
		} else {
			loadedHolders = append(loadedHolders, nd)
		}
	}

	// order the holders by their position in the sorted array
	sortNodesByRef(healthyHolders, available.SortedNodes)
	sortNodesByRef(loadedHolders, available.SortedNodes)

	if len(healthyHolders) > 0 {
		// a healthy node already has the cache: send straight to it, no pull
		return &node.RouterRes{NodeD: healthyHolders[0], NodeP: node.Node{}, MetaData: []string{}} // TODO: get metadata we need
	}
	if len(loadedHolders) > 0 && r.req.pullWorthIt(R.matched) {
		// no healthy holder, but the matched prefix is long enough that pulling
		// from a loaded holder beats recomputing it on the destination:
		// send to the healthiest node and have it pull from the least loaded holder
		return &node.RouterRes{NodeD: dest, NodeP: loadedHolders[0], MetaData: []string{}} // TODO: get metadata we need
	}
	// no holder we should pull from: recompute on the healthiest node
	return &node.RouterRes{NodeD: dest, NodeP: node.Node{}, MetaData: []string{}} // TODO: get metadata we need
}

func (r *RRRouter) router(ctx context.Context, R *hasherRes) *node.RouterRes {
	// does a round robin sitiation, no matter if the node is healthy or not (in the experimental setting, we will only have
	// "healthy" nodes, the better statement here is no matter if the node is loaded or not)
	// we need to maintain the last used node

	r.req.routeTable.RRMap.Mx.Lock()
	defer r.req.routeTable.RRMap.Mx.Unlock()
	if r.req.routeTable.RRMap.CurrentIndex == -1 {
		// this is the first req
		// use the 0th one lol
		r.req.routeTable.RRMap.CurrentIndex = 0
		return &node.RouterRes{NodeD: r.req.routeTable.Nodes[0]}
	}
	next := nextNode(r.req.routeTable.RRMap.TotalNodes, r.req.routeTable.RRMap.CurrentIndex)
	r.req.routeTable.RRMap.CurrentIndex = next
	return &node.RouterRes{NodeD: r.req.routeTable.Nodes[next]}
}

func nextNode(size int, curr int) int {
	if curr == size-1 {
		return 0
	}
	return curr + 1
}

func (r *CacheRouter) router(ctx context.Context, R *hasherRes) *node.RouterRes {
	// implement the CacheRoute logic based on the load events we get from healthyNodes
	return nil
}

func (r *LLRouter) router(ctx context.Context, R *hasherRes) *node.RouterRes {
	// the least loaded node
	return r.req.healthTable.LLRoute()
}

//
// type Params struct {
// }
//
// type NodeState struct {
// 	Name       string
// 	MatchedTok int     // longest prefix match, in tokens (matched pages x page size)
// 	KVUsage    float64 // L(I_k): allocated / total KV, in [0,1]
// 	Queue      float64 // Q(I_k): waiting requests
// }
//
// func clip(x, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, x)) }
//
// func score(p Params, nTokens int, n NodeState) float64 {
// 	c := 0.0
// 	if nTokens > 0 {
// 		c = clip(float64(n.MatchedTok)/float64(nTokens), 0, 1) // eq 2
// 	}
// 	tHat := p.A*float64(nTokens)*(1-c) + p.B*n.Queue + p.C // eq 3
// 	t := clip(1-tHat/p.TMax, 0, 1)                         // eq 4
// 	return p.Alpha*c + p.Beta*(1-n.KVUsage) + p.Gamma*t    // eq 1
// }
//
// func pick(p Params, nTokens int, nodes []NodeState) string {
// 	best, bestScore := "", math.Inf(-1)
// 	for _, n := range nodes {
// 		if s := score(p, nTokens, n); s > bestScore {
// 			best, bestScore = n.Name, s
// 		}
// 	}
// 	return best
// }
//
// type Sample struct {
// 	NTokens   int
// 	CacheFrac float64 // C at send time
// 	Queue     float64 // Q at send time
// 	TTFT      float64 // observed, seconds
// }
//
// type Coeffs struct{ A, B, C, TMax float64 }
//
// // Least-squares fit of ttft = a*n*(1-C) + b*Q + c, plus p99 TTFT.
// func Calibrate(s []Sample) (Coeffs, error) {
// 	if len(s) < 3 {
// 		return Coeffs{}, errors.New("need at least 3 samples")
// 	}
// 	var xtx [3][3]float64
// 	var xty [3]float64
// 	ttfts := make([]float64, 0, len(s))
// 	for _, v := range s {
// 		x := [3]float64{float64(v.NTokens) * (1 - v.CacheFrac), v.Queue, 1}
// 		for i := 0; i < 3; i++ {
// 			for j := 0; j < 3; j++ {
// 				xtx[i][j] += x[i] * x[j]
// 			}
// 			xty[i] += x[i] * v.TTFT
// 		}
// 		ttfts = append(ttfts, v.TTFT)
// 	}
// 	beta, err := solve3(xtx, xty)
// 	if err != nil {
// 		return Coeffs{}, err
// 	}
// 	sort.Float64s(ttfts)
// 	idx := int(math.Ceil(0.99*float64(len(ttfts)))) - 1
// 	if idx < 0 {
// 		idx = 0
// 	}
// 	return Coeffs{beta[0], beta[1], beta[2], ttfts[idx]}, nil
// }
//
// func solve3(m [3][3]float64, y [3]float64) ([3]float64, error) {
// 	var a [3][4]float64
// 	for i := 0; i < 3; i++ {
// 		copy(a[i][:3], m[i][:])
// 		a[i][3] = y[i]
// 	}
// 	for col := 0; col < 3; col++ {
// 		p := col
// 		for r := col + 1; r < 3; r++ {
// 			if math.Abs(a[r][col]) > math.Abs(a[p][col]) {
// 				p = r
// 			}
// 		}
// 		if math.Abs(a[p][col]) < 1e-12 {
// 			return [3]float64{}, errors.New("singular: calibration samples need more variety")
// 		}
// 		a[col], a[p] = a[p], a[col]
// 		for r := col + 1; r < 3; r++ {
// 			f := a[r][col] / a[col][col]
// 			for k := col; k < 4; k++ {
// 				a[r][k] -= f * a[col][k]
// 			}
// 		}
// 	}
// 	var x [3]float64
// 	for i := 2; i >= 0; i-- {
// 		sum := a[i][3]
// 		for k := i + 1; k < 3; k++ {
// 			sum -= a[i][k] * x[k]
// 		}
// 		x[i] = sum / a[i][i]
// 	}
// 	return x, nil
// }
