package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/vimeo/galaxycache"
	"github.com/vimeo/galaxycache/compattest/peercfg"
	"github.com/vimeo/galaxycache/consistenthash/chtest"
	gcgrpc "github.com/vimeo/galaxycache/grpc"
	gchttp "github.com/vimeo/galaxycache/http"
)

func readCfg() (peercfg.Config, error) {
	cfg := peercfg.Config{}

	// Use a decoder so we can use json's self-delimiting nature to send multiple payloads later
	dec := json.NewDecoder(os.Stdin)
	if decErr := dec.Decode(&cfg); decErr != nil {
		return peercfg.Config{}, fmt.Errorf("failed to parse config from stdin: %w", decErr)
	}

	return cfg, nil
}

func run() error {
	cfg, cfgErr := readCfg()
	if cfgErr != nil {
		return fmt.Errorf("init config failed: %w", cfgErr)
	}

	notCh := make(chan os.Signal, 1)
	signal.Notify(notCh, os.Interrupt, syscall.SIGTERM)

	// grab fd 3, which should have been passed down by our process parent
	listenerFile := os.NewFile(3, "localhost:0")
	l, fileLisErr := net.FileListener(listenerFile)
	if fileLisErr != nil {
		return fmt.Errorf("failed to init file listener with FD3: %w", fileLisErr)
	}
	listenerFile.Close()

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

	fp := galaxycache.FetchProtocol(nil)

	switch cfg.FetchProto {
	case peercfg.FetchGRPC:
		fp = gcgrpc.NewGRPCFetchProtocol(grpc.WithTransportCredentials(insecure.NewCredentials()))
	case peercfg.FetchHTTP:
		fp = gchttp.NewHTTPFetchProtocol(nil)
	}
	u := galaxycache.NewUniverse(fp, cfg.SelfName, galaxycache.WithHashOpts(&galaxycache.HashOptions{
		Replicas: hrArgs.NSegsPerKey,
		HashFn:   hrArgs.HashFunc,
	}))
	u.SetPeers(gcPeers...)
	u.SetIncludeSelf(cfg.IncSelf)

	for _, galaxy := range cfg.Galaxies {
		u.NewGalaxy(galaxy.Name, int64(galaxy.Bytes), galaxycache.GetterFunc(func(ctx context.Context, key string, codec galaxycache.Codec) error {
			switch galaxy.HydrationMode {
			case peercfg.HandlerFail:
				return errors.New("not a useful error")
			case peercfg.HandlerGRPCNotFound:
				return status.Errorf(codes.NotFound, "failed to extract key %q", key)
			case peercfg.HandlerNotFound:
				panic("unimplemented")
			case peercfg.HandlerSuccess:
				out := bytes.Buffer{}
				if galaxy.PrefixSelfName {
					out.WriteString(cfg.SelfName)
					out.WriteString(": ")
				}
				if galaxy.EchoKey {
					out.WriteString(key)
					out.WriteString(": ")
				}
				out.WriteString("{some value}")
				if unmarErr := codec.UnmarshalBinary(out.Bytes()); unmarErr != nil {
					return fmt.Errorf("unmarshal failed: %w", unmarErr)
				}
				return nil
			default:
			}
			return nil
		}))
	}

	wg := sync.WaitGroup{}
	gSrv := grpc.NewServer()
	gcgrpc.RegisterGRPCServer(u, gSrv)

	gchttp.RegisterHTTPHandler(u, nil, nil)
	httpSrv := http.Server{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-notCh
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
			if srvErr := gSrv.Serve(l); srvErr != nil {
				fmt.Fprintf(os.Stderr, "gRPC server exited unsuccessfully: %s", srvErr)
			}
		}()
	case peercfg.ListenHTTP:
		wg.Add(1)
		go func() {
			defer wg.Done()
			if srvErr := httpSrv.Serve(l); srvErr != nil && srvErr != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "http server exited unsuccessfully: %s", srvErr)
			}
		}()

	case peercfg.ListenGRPCHTTP:
		wg.Add(1)
		go func() {
			defer wg.Done()
			if srvErr := gSrv.Serve(l); srvErr != nil {
				fmt.Fprintf(os.Stderr, "gRPC server exited unsuccessfully: %s", srvErr)
			}
		}()
		// grab fd 4, which should have been passed down by our process parent
		listenerFile4 := os.NewFile(4, "localhost:0")
		l4, fileLis4Err := net.FileListener(listenerFile4)
		if fileLis4Err != nil {
			return fmt.Errorf("failed to init file listener with FD3: %w", fileLis4Err)
		}
		listenerFile4.Close()
		wg.Add(1)
		go func() {
			defer wg.Done()
			if srvErr := httpSrv.Serve(l4); srvErr != nil && srvErr != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "http server exited unsuccessfully: %s", srvErr)
			}
		}()
	}

	wg.Wait()
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "failure: %s", err)
		os.Exit(1)
	}

}
