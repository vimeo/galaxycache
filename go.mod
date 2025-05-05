module github.com/vimeo/galaxycache

go 1.23.8

require (
	github.com/golang/protobuf v1.5.4
	github.com/vimeo/go-clocks v1.3.0
	go.opencensus.io v0.22.5
	golang.org/x/sync v0.13.0
	google.golang.org/grpc v1.72.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/vimeo/galaxycache/compattest/peercfg v0.0.0-00010101000000-000000000000 // indirect
	golang.org/x/net v0.39.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
	golang.org/x/text v0.24.0 // indirect
	google.golang.org/genproto v0.0.0-20250425173222-7b384671a197 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250414145226-207652e42e2e // indirect
)

replace github.com/vimeo/galaxycache/compattest/peercfg => ./compattest/peercfg
