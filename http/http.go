/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	gc "github.com/vimeo/galaxycache"

	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats"
)

const defaultBasePath = "/_galaxycache/"
const defaultPeekBasePath = "/_galaxycache_peek/"

// HTTPFetchProtocol specifies HTTP specific options for HTTP-based
// peer communication
type HTTPFetchProtocol struct {
	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	transport    http.RoundTripper
	basePath     string
	peekBasePath string
}

// HTTPOptions can specify the transport, base path, peek base path, and
// stats.Recorder for serving and fetching.
// *ONLY SPECIFY the base path IF NOT USING THE DEFAULT "/_galaxycache/" BASE PATH*.
// *ONLY SPECIFY the peek base path IF NOT USING THE DEFAULT "/_galaxycache_peek/" BASE PATH*.
type HTTPOptions struct {
	Transport    http.RoundTripper
	BasePath     string
	PeekBasePath string
	Recorder     stats.Recorder
}

// NewHTTPFetchProtocol creates an HTTP fetch protocol to be passed
// into a Universe constructor; uses a user chosen base path specified
// in HTTPOptions (or the default "/_galaxycache/" base path if passed nil).
// *You must use the same base path for the HTTPFetchProtocol and the
// HTTPHandler on the same Universe*.
func NewHTTPFetchProtocol(opts *HTTPOptions) *HTTPFetchProtocol {
	newProto := &HTTPFetchProtocol{
		basePath:     defaultBasePath,
		peekBasePath: defaultPeekBasePath,
	}

	if opts == nil {
		newProto.transport = &ochttp.Transport{}
		return newProto
	}
	newProto.transport = &ochttp.Transport{
		Base: opts.Transport,
	}
	if opts.BasePath != "" {
		newProto.basePath = opts.BasePath
	}
	if opts.PeekBasePath != "" {
		newProto.peekBasePath = opts.PeekBasePath
	}

	return newProto
}

// NewFetcher implements the Protocol interface for HTTPProtocol by constructing
// a new fetcher to fetch from peers via HTTP
// Prefixes URL with http:// if neither http:// nor https:// are prefixes of
// the URL argument.
func (hp *HTTPFetchProtocol) NewFetcher(url string) (gc.RemoteFetcher, error) {
	if !(strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://")) {
		url = "http://" + url
	}
	return &httpFetcher{transport: hp.transport, baseURL: url + hp.basePath}, nil
}

// HTTPHandler implements the HTTP handler necessary to serve an HTTP
// request; it contains a pointer to its parent Universe in order to access
// its galaxies
type HTTPHandler struct {
	universe     *gc.Universe
	basePath     string
	peekBasePath string
	recorder     stats.Recorder
}

// RegisterHTTPHandler sets up an HTTPHandler with a user specified path
// and serveMux (if non nil) to handle requests to the given Universe.
// If both opts and serveMux are nil, defaultBasePath and DefaultServeMux
// will be used. *You must use the same base path for the HTTPFetchProtocol
// and the HTTPHandler on the same Universe*.
//
// If a serveMux is not specified, opencensus metrics will automatically
// wrap the handler. It is recommended to configure opencensus yourself
// if specifying a serveMux.
func RegisterHTTPHandler(universe *gc.Universe, opts *HTTPOptions, serveMux *http.ServeMux) {
	basePath := defaultBasePath
	peekBasePath := defaultPeekBasePath
	var recorder stats.Recorder
	if opts != nil {
		if opts.BasePath != "" {
			basePath = opts.BasePath
		}
		if opts.PeekBasePath != "" {
			peekBasePath = opts.PeekBasePath
		}
		recorder = opts.Recorder
	}
	newHTTPHandler := &HTTPHandler{
		basePath:     basePath,
		peekBasePath: peekBasePath,
		universe:     universe,
		recorder:     recorder,
	}
	if serveMux == nil {
		http.Handle(basePath, &ochttp.Handler{
			Handler: ochttp.WithRouteTag(newHTTPHandler, basePath),
		})
		http.Handle(peekBasePath, &ochttp.Handler{
			Handler: ochttp.WithRouteTag(newHTTPHandler, peekBasePath),
		})
	} else {
		serveMux.Handle(basePath, ochttp.WithRouteTag(newHTTPHandler, basePath))
		serveMux.Handle(peekBasePath, ochttp.WithRouteTag(newHTTPHandler, peekBasePath))
	}
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	peek := false
	// Parse request.
	switch {
	case strings.HasPrefix(r.URL.Path, h.basePath):
		peek = false
	case strings.HasPrefix(r.URL.Path, h.peekBasePath):
		peek = true
	default:
		panic("HTTPHandler serving unexpected path: " + r.URL.Path)
	}
	strippedPath := r.URL.Path[len(h.basePath):]
	needsUnescaping := false
	if r.URL.RawPath != "" && r.URL.RawPath != r.URL.Path {
		strippedPath = r.URL.RawPath[len(h.basePath):]
		needsUnescaping = true
	}
	parts := strings.SplitN(strippedPath, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	galaxyName := parts[0]
	key := parts[1]

	if needsUnescaping {
		gn, gnUnescapeErr := url.PathUnescape(galaxyName)
		if gnUnescapeErr != nil {
			http.Error(w, fmt.Sprintf("failed to unescape galaxy name %q: %s", galaxyName, gnUnescapeErr), http.StatusBadRequest)
			return
		}
		k, keyUnescapeErr := url.PathUnescape(key)
		if keyUnescapeErr != nil {
			http.Error(w, fmt.Sprintf("failed to unescape key %q: %s", key, keyUnescapeErr), http.StatusBadRequest)
			return
		}
		galaxyName, key = gn, k
	}

	// Fetch the value for this galaxy/key.
	galaxy := h.universe.GetGalaxy(galaxyName)
	if galaxy == nil {
		w.Header().Set(galaxyPresentHeader, galaxyStatusNotFound)
		http.Error(w, "no such galaxy: "+galaxyName, http.StatusNotFound)
		return
	}
	w.Header().Set(galaxyPresentHeader, galaxyStatusFound)

	ctx := r.Context()

	// TODO: remove galaxy.Stats from here
	galaxy.Stats.ServerRequests.Add(1)
	stats.RecordWithOptions(
		ctx,
		stats.WithMeasurements(gc.MServerRequests.M(1)),
		stats.WithRecorder(h.recorder),
	)

	fetchMode := gc.FetchModeNoPeerBackend
	if peek {
		fetchMode = gc.FetchModePeek
	}

	var value gc.ByteCodec
	_, err := galaxy.GetWithOptions(ctx, gc.GetOptions{FetchMode: fetchMode}, key, &value)
	if err != nil {
		if nfErr := gc.NotFoundErr(nil); errors.As(err, &nfErr) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(value)
}

const galaxyPresentHeader = "X-Galaxycache-Galaxy-Status"

const galaxyStatusFound = "Found"
const galaxyStatusNotFound = "Not Found"

type httpFetcher struct {
	transport   http.RoundTripper
	baseURL     string
	basePeekURL string
}

// Fetch here implements the RemoteFetcher interface for sending a GET request over HTTP to a peer
func (h *httpFetcher) Fetch(ctx context.Context, galaxy string, key string) ([]byte, error) {
	return h.fetch(ctx, false, galaxy, key)
}

// Peek here implements the RemoteFetcher interface for sending a GET request over HTTP to a peer
func (h *httpFetcher) Peek(ctx context.Context, galaxy string, key string) ([]byte, error) {
	return h.fetch(ctx, true, galaxy, key)
}

// fetch backs both Fetch and Peek
func (h *httpFetcher) fetch(ctx context.Context, peek bool, galaxy string, key string) ([]byte, error) {
	baseURL := h.baseURL
	if peek {
		baseURL = h.basePeekURL
	}
	u := fmt.Sprintf(
		"%v%v/%v",
		baseURL,
		url.PathEscape(galaxy),
		url.PathEscape(key),
	)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	res, err := h.transport.RoundTrip(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusNotFound:
		switch galaxyPresent := res.Header.Get(galaxyPresentHeader); galaxyPresent {
		case galaxyStatusNotFound:
			return nil, errors.New("galaxy not found")
		case galaxyStatusFound:
			return nil, fmt.Errorf("key not found: %w", gc.TrivialNotFoundErr{})
		default:
			// anything else, including a missing header (either an
			// older version of galaxycache, or the HTTP handler's
			// not registered for that path)
			return nil, fmt.Errorf("server returned HTTP response status code: %v; %q header: %q",
				res.Status, galaxyPresentHeader, galaxyPresent)
		}
	case http.StatusOK:
		data, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, fmt.Errorf("reading response body: %v", err)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("server returned HTTP response status code: %v", res.Status)
	}
}

// Close here implements the RemoteFetcher interface for closing (does nothing for HTTP)
func (h *httpFetcher) Close() error {
	return nil
}
