package driver

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/sacloud/libsacloud/api"
	"github.com/sacloud/libsacloud/sacloud"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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

	accessToken       string
	accessTokenSecret string
	zone              string

	sakuraClient    *api.Client
	sakuraNFSClient nfsAPIClient

	srv *grpc.Server
	log *logrus.Entry
}

type nfsAPIClient interface {
	Find() (*api.SearchNFSResponse, error)
	SetEmpty()
	SetNameLike(name string)
	Create(value *sacloud.NFS) (*sacloud.NFS, error)

	SleepUntilUp(id int64, timeout time.Duration) error
	SleepUntilDown(id int64, timeout time.Duration) error
	SleepWhileCopying(id int64, timeout time.Duration, maxRetry int) error
}

// NewDriver returns a CSI plugin that contains the necessary gRPC
// interfaces to interact with Kubernetes over unix domain sockets for
// manage SakuraCloud NFS storage
func NewDriver(ep, token, secret, zone string) (*Driver, error) {

	client := api.NewClient(token, secret, zone)

	return &Driver{
		endpoint:          ep,
		accessToken:       token,
		accessTokenSecret: secret,
		zone:              zone,
		sakuraClient:      client,
		sakuraNFSClient:   client.GetNFSAPI(),
		log: logrus.New().WithFields(logrus.Fields{
			"zone": zone,
		}),
	}, nil
}

// Run starts the CSI plugin by communication over the given endpoint
func (d *Driver) Run() error {
	u, err := url.Parse(d.endpoint)
	if err != nil {
		return fmt.Errorf("unable to parse address: %q", err)
	}

	addr := path.Join(u.Host, filepath.FromSlash(u.Path))
	if u.Host == "" {
		addr = filepath.FromSlash(u.Path)
	}

	// CSI plugins talk only over UNIX sockets currently
	if u.Scheme != "unix" {
		return fmt.Errorf("currently only unix domain sockets are supported, have: %s", u.Scheme)
	} else {
		// remove the socket if it's already there. This can happen if we
		// deploy a new version and the socket was created from the old running
		// plugin.
		d.log.WithField("socket", addr).Info("removing socket")
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove unix domain socket file %s, error: %s", addr, err)
		}
	}

	listener, err := net.Listen(u.Scheme, addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	// log response errors for better observability
	errHandler := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			d.log.WithError(err).WithField("method", info.FullMethod).Error("method failed")
		}
		return resp, err
	}

	d.srv = grpc.NewServer(grpc.UnaryInterceptor(errHandler))
	csi.RegisterIdentityServer(d.srv, d)
	csi.RegisterControllerServer(d.srv, d)

	// **Note**
	// NodeServer is not implemented. use NFS node driver
	// (https://github.com/kubernetes-csi/drivers/tree/master/pkg/nfs)

	//csi.RegisterNodeServer(d.srv, d)

	d.log.WithField("addr", addr).Info("server started")
	return d.srv.Serve(listener)

}

// Stop stops the plugin
func (d *Driver) Stop() {
	d.log.Info("server stopped")
	d.srv.Stop()
}
