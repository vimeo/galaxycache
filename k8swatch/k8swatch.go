package k8swatch

import (
	"context"
	"net"

	"github.com/vimeo/galaxycache"
	"github.com/vimeo/k8swatcher"
)

// UpdateCB returns a callback that's appropriate for incrementally
// updating the set of peers based on events from [k8swatcher.PodWatcher].
// The port should be the integer port-number on which this galaxycache
// universe is registered with a gRPC server (or similar).
func UpdateCB(u *galaxycache.Universe, port int) k8swatcher.EventCallback {
	return func(ctx context.Context, event k8swatcher.PodEvent) {
		switch ut := event.(type) {
		case *k8swatcher.CreatePod:
			if ut.IP != nil && ut.IsReady() {
				addr := net.TCPAddr{IP: ut.IP.IP, Port: port}
				u.AddPeer(galaxycache.Peer{
					ID:  ut.PodName(),
					URI: addr.String(),
				})
			} else {
				u.RemovePeers(ut.PodName())
			}
		case *k8swatcher.ModPod:
			if ut.IP != nil && ut.IsReady() {
				addr := net.TCPAddr{IP: ut.IP.IP, Port: port}
				u.AddPeer(galaxycache.Peer{
					ID:  ut.PodName(),
					URI: addr.String(),
				})
			} else {
				u.RemovePeers(ut.PodName())
			}
		case *k8swatcher.DeletePod:
			u.RemovePeers(ut.PodName())
		case *k8swatcher.InitialListComplete:
			return
		}
	}

}
