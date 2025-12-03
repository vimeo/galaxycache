module github.com/vimeo/galaxycache/compattest/peerv1.2

go 1.24.0

require (
	github.com/vimeo/galaxycache v1.2.2
	github.com/vimeo/galaxycache/compattest/peercfg v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.77.0
)

require (
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/vimeo/go-clocks v1.1.2 // indirect
	go.opencensus.io v0.22.5 // indirect
	golang.org/x/net v0.46.1-0.20251013234738-63d1a5100f82 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/text v0.30.0 // indirect
	google.golang.org/genproto v0.0.0-20251111163417-95abcf5c77ba // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251103181224-f26f9409b101 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace github.com/vimeo/galaxycache/compattest/peercfg => ../peercfg
