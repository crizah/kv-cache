package handlers

import "net/http"

func ReqHandler(w http.ResponseWriter, r *http.Request) {
	// we get the request
	// we tokenise it
	//
	// we hash it
	// we look up in our route table
	// while we are waiting to get a cache hit or miss, we lookup our endpoints, see which are available at all (no routing here, just get the
	// available nodes with the last used timestamps)
	// once we get the hashes, we determise if we have a cache hit or a cache miss
	// if we have a cache hit, we see if the node it mentions is available, (use channels for this cuz i want to (the routine/function returning the node to
	// use blocks and waits for the goroutine that gives us the available nodes, though i doubt that will happen, most likely the writer will block
	// till somebody is there to listen))
	//
	// we then send our request (and depending on the node type (home or new), we send metdata)
	// wait for the response
	//

}
