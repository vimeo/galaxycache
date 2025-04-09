package k8swatch

import (
	"context"
	"net"

	"github.com/vimeo/galaxycache"
	"github.com/vimeo/k8swatcher"

	k8score "k8s.io/api/core/v1"
)

func handleSelfUpdate(u *galaxycache.Universe, podDef *k8score.Pod) {
	if podDef.DeletionTimestamp == nil {
		return
	}
	// We're going down, remove ourselves from the hashring
	u.SetIncludeSelf(false)
}

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
	// Use a type-assert so we can handle some version skew with older
	// versions of galaxycache (until we have a new release and update the
	// k8swatch go.mod to require it)
	isSelf := func(event k8swatcher.PodEvent) bool { return false }
	if ui, ok := any(u).(interface{ SelfID() string }); ok {
		selfID := ui.SelfID()
		isSelf = func(event k8swatcher.PodEvent) bool { return event.PodName() == selfID }
	}
	return func(ctx context.Context, event k8swatcher.PodEvent) {
		switch ut := event.(type) {
		case *k8swatcher.CreatePod:
			if isSelf(event) {
				handleSelfUpdate(u, ut.Def)
				return
			}
			handlePodUpdate(u, event, port, ut.IsReady(), ut.IP)
		case *k8swatcher.ModPod:
			if isSelf(event) {
				handleSelfUpdate(u, ut.Def)
				return
			}
			handlePodUpdate(u, event, port, ut.IsReady(), ut.IP)
		case *k8swatcher.DeletePod:
			if isSelf(event) {
				// shouldn't happen, but handle it anyway
				u.SetIncludeSelf(false)
				return
			}
			u.RemovePeers(ut.PodName())
		case *k8swatcher.InitialListComplete:
			return
		}
	}

}
