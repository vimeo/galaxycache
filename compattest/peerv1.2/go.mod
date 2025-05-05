module github.com/vimeo/galaxycache/compattest/peerv1.2

go 1.23.8

require (
	github.com/vimeo/galaxycache v1.2.2
	github.com/vimeo/galaxycache/compattest/peercfg v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.72.0
)

require (
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/vimeo/go-clocks v1.1.2 // indirect
	go.opencensus.io v0.22.5 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250428153025-10db94c68c34 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)

replace github.com/vimeo/galaxycache/compattest/peercfg => ../peercfg
