package detector

import (
	"github.com/xmwilldo/karmada-ke-agent/pkg/util"
	"github.com/xmwilldo/karmada-ke-agent/pkg/util/informermanager/keys"
)

// ClusterWideKeyFunc generates a ClusterWideKey for object.
func ClusterWideKeyFunc(obj interface{}) (util.QueueKey, error) {
	return keys.ClusterWideKeyFunc(obj)
}
