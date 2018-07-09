package driver

import (
	// csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/sacloud/libsacloud/api"
	"google.golang.org/grpc"
	"github.com/sirupsen/logrus"
)

const (
	driverName    = "jp.ne.sakura.csi.nfs"
	vendorVersion = "0.0.1"
)

// Driver implements the following CSI interfaces:
//
//   csi.IdentityServer
//   csi.ControllerServer
//   csi.NodeServer
//
type Driver struct {
	endpoint string

	accessToken string
	accessTokenSecret string
	zone string

	srv      *grpc.Server
	sakuracloudClient *api.Client
	log      *logrus.Entry
}

// NewDriver returns a CSI plugin that contains the necessary gRPC
// interfaces to interact with Kubernetes over unix domain sockets for
// manage SakuraCloud NFS storage
func NewDriver(ep, token, secret , zone string) (*Driver, error) {

	// TODO init SakuraCloud API Client

	return &Driver{
		endpoint: ep,
		accessToken: token,
		accessTokenSecret: secret,
		zone: zone,
		log: logrus.New().WithFields(logrus.Fields{
		"zone":  zone,
	}),
	}, nil
}

// Run starts the CSI plugin by communication over the given endpoint
func (d *Driver) Run() error {
	// TODO not implements
	return nil
}

// Stop stops the plugin
func (d *Driver) Stop() {
	// TODO not implements
}
