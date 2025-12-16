module github.com/vimeo/galaxycache

go 1.24.10

require (
	github.com/golang/protobuf v1.5.4
	github.com/vimeo/galaxycache/compattest/peercfg v0.0.0-20251208211800-872fc1d003d6
	github.com/vimeo/go-clocks v1.3.0
	go.opencensus.io v0.22.5
	golang.org/x/sync v0.17.0
	google.golang.org/grpc v1.77.0
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	golang.org/x/net v0.46.1-0.20251013234738-63d1a5100f82 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/text v0.30.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251022142026-3a174f9686a8 // indirect
)

replace github.com/vimeo/galaxycache/compattest/peercfg => ./compattest/peercfg
