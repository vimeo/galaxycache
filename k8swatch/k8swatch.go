package k8swatch

import (
	"context"
	"net"

	"github.com/vimeo/galaxycache"
	"github.com/vimeo/k8swatcher"
)

func handlePodUpdate(u *galaxycache.Universe, event k8swatcher.PodEvent, port int, isReady bool, podIP *net.IPAddr) {
	if podIP != nil && isReady {
		addr := net.TCPAddr{IP: podIP.IP, Port: port}
		u.AddPeer(galaxycache.Peer{
			ID:  event.PodName(),
			URI: addr.String(),
		})
	} else {
		u.RemovePeers(event.PodName())
	}
}

// UpdateCB returns a callback that's appropriate for incrementally
// updating the set of peers based on events from [k8swatcher.PodWatcher].
// The port should be the integer port-number on which this galaxycache
// universe is registered with a gRPC server (or similar).
func UpdateCB(u *galaxycache.Universe, port int) k8swatcher.EventCallback {
	return func(ctx context.Context, event k8swatcher.PodEvent) {
		switch ut := event.(type) {
		case *k8swatcher.CreatePod:
			handlePodUpdate(u, event, port, ut.IsReady(), ut.IP)
		case *k8swatcher.ModPod:
			handlePodUpdate(u, event, port, ut.IsReady(), ut.IP)
		case *k8swatcher.DeletePod:
			u.RemovePeers(ut.PodName())
		case *k8swatcher.InitialListComplete:
			return
		}
	}

}
