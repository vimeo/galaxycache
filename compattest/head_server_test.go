package compattest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/vimeo/galaxycache"
	"github.com/vimeo/galaxycache/compattest/peercfg"
	"github.com/vimeo/galaxycache/consistenthash/chtest"
	gcgrpc "github.com/vimeo/galaxycache/grpc"
	gchttp "github.com/vimeo/galaxycache/http"
)

func genBackendGetter(galaxy peercfg.Galaxy, selfName string) galaxycache.GetterFuncWithInfo {
	return func(ctx context.Context, key string, codec galaxycache.Codec) (galaxycache.BackendGetInfo, error) {
		switch galaxy.HydrationMode {
		case peercfg.HandlerFail:
			return galaxycache.BackendGetInfo{}, errors.New("not a useful error")
		case peercfg.HandlerGRPCNotFound:
			return galaxycache.BackendGetInfo{}, status.Errorf(codes.NotFound, "failed to extract key %q", key)
		case peercfg.HandlerNotFound:
			return galaxycache.BackendGetInfo{}, fmt.Errorf("key %w", galaxycache.TrivialNotFoundErr{})
		case peercfg.HandlerSuccess:
			out := bytes.Buffer{}
			if galaxy.PrefixSelfName {
				out.WriteString(selfName)
				out.WriteString(": ")
			}
			if galaxy.EchoKey {
				out.WriteString(key)
				out.WriteString(": ")
			}
			out.WriteString("{some value}")
			if unmarErr := codec.UnmarshalBinary(out.Bytes()); unmarErr != nil {
				return galaxycache.BackendGetInfo{}, fmt.Errorf("unmarshal failed: %w", unmarErr)
			}
			return galaxycache.BackendGetInfo{Expiration: galaxy.Expiry}, nil
		default:
		}
		return galaxycache.BackendGetInfo{}, nil
	}
}

func genUniverse(cfg peercfg.Config) (*galaxycache.Universe, []*galaxycache.Galaxy) {
	gcPeers := []galaxycache.Peer{}
	peerNames := []string{}
	// add the relevant peers
	for _, p := range cfg.Peers {
		peerNames = append(peerNames, p.Name)
		if p.Name == cfg.SelfName {
			continue
		}
		gcPeers = append(gcPeers, galaxycache.Peer{
			ID:  p.Name,
			URI: p.Address,
		})
	}

	preRegKeys := make(map[string][]string, len(cfg.PreRegisterKeys))
	for k, v := range cfg.PreRegisterKeys {
		preRegKeys[k] = []string{v}
	}

	hrArgs := chtest.NewMapArgs(chtest.Args{Owners: peerNames, RegisterKeys: preRegKeys})

	fp := defaultFetchProtocol(cfg.FetchProto)
	u := galaxycache.NewUniverse(fp, cfg.SelfName, galaxycache.WithHashOpts(&galaxycache.HashOptions{
		Replicas: hrArgs.NSegsPerKey,
		HashFn:   hrArgs.HashFunc,
	}))
	u.SetPeers(gcPeers...)
	u.SetIncludeSelf(cfg.IncSelf)
	gs := make([]*galaxycache.Galaxy, len(cfg.Galaxies))
	for i, galaxy := range cfg.Galaxies {
		gOpts := []galaxycache.GalaxyOption{}
		if galaxy.Peek != nil {
			gOpts = append(gOpts, galaxycache.WithPreviousPeerPeeking(galaxycache.PeekPeerCfg{
				PeekTimeout: galaxy.Peek.Timeout,
				WarmTime:    galaxy.Peek.WarmTime,
			}))
		}
		gs[i] = u.NewGalaxyWithBackendInfo(galaxy.Name, int64(galaxy.Bytes), genBackendGetter(galaxy, cfg.SelfName), gOpts...)
	}
	return u, gs
}

func runPeer(ctx context.Context, t testing.TB, u *galaxycache.Universe, priL, secL net.Listener, cfg peercfg.Config) error {

	wg := sync.WaitGroup{}
	gSrv := grpc.NewServer()
	gcgrpc.RegisterGRPCServer(u, gSrv)

	mux := http.ServeMux{}
	gchttp.RegisterHTTPHandler(u, nil, &mux)
	httpSrv := http.Server{
		Handler:  &mux,
		ErrorLog: log.New(&prefixedTestLogWriter{t: t, prefix: "http: "}, cfg.SelfName+": ", log.Ltime|log.Ldate|log.Lmicroseconds),
		ConnState: func(conn net.Conn, cs http.ConnState) {
			t.Logf("%s: connection from %s to %s changed state to %s", cfg.SelfName, conn.RemoteAddr(), conn.LocalAddr(), cs)
		},
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-ctx.Done()
		t.Log("context canceled")
		gSrv.GracefulStop()
		shutdownCtx, shCancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer shCancel()
		httpSrv.Shutdown(shutdownCtx)
	}()

	switch cfg.ListenMode {
	case peercfg.ListenGRPC:
		wg.Add(1)
		go func() {
			defer wg.Done()
			if srvErr := gSrv.Serve(priL); srvErr != nil {
				fmt.Fprintf(os.Stderr, "gRPC server exited unsuccessfully: %s", srvErr)
			}
		}()
	case peercfg.ListenHTTP:
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Logf("%s: starting HTTP server", cfg.SelfName)
			if srvErr := httpSrv.Serve(priL); srvErr != nil && srvErr != http.ErrServerClosed {
				t.Logf("http server exited unsuccessfully: %s", srvErr)
			}
			t.Logf("%s: http server exited", cfg.SelfName)
		}()

	case peercfg.ListenGRPCHTTP:
		wg.Add(1)
		go func() {
			defer wg.Done()
			if srvErr := gSrv.Serve(priL); srvErr != nil {
				fmt.Fprintf(os.Stderr, "gRPC server exited unsuccessfully: %s", srvErr)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			if srvErr := httpSrv.Serve(secL); srvErr != nil && srvErr != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "http server exited unsuccessfully: %s", srvErr)
			}
		}()
	}

	wg.Wait()
	if shutErr := u.Shutdown(); shutErr != nil {
		return fmt.Errorf("failed to shutdown universe: %w", shutErr)
	}
	return nil
}
