package driver

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/sacloud/libsacloud/api"
	"github.com/sacloud/libsacloud/sacloud"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	logrus.SetOutput(ioutil.Discard)
}

var (
	fakeNFSClient = &fakeNFSAPIClient{}

	fakeDriver = &Driver{
		log: logrus.NewEntry(logrus.StandardLogger()),
	}
)

func TestDriver_CreateVolume(t *testing.T) {

	testCases := []struct {
		name           string
		nfsAPIClient   nfsAPIClient
		request        *csi.CreateVolumeRequest
		expectResponse *csi.CreateVolumeResponse
		expectHasError bool
	}{
		{
			name: "request parameter don't have valid volume capability",
			request: &csi.CreateVolumeRequest{
				Name: "fake",
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: 100 * GiB,
				},
			},
			expectHasError: true,
		},
		{
			name: "RequiredBytes is out of ranges",
			request: &csi.CreateVolumeRequest{
				Name: "fake",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: 10 * GiB,
				},
			},
			expectHasError: true,
		},
		{
			name: "NFS volume don't have CSI marker tags",
			nfsAPIClient: &fakeNFSAPIClient{
				findResponse: &api.SearchNFSResponse{
					NFS: []sacloud.NFS{
						buildFakeNFS(1, &sacloud.CreateNFSValue{
							Name: "fake",
						}),
					},
				},
			},
			request: &csi.CreateVolumeRequest{
				Name: "fake",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: 100 * GiB,
					LimitBytes:    100 * GiB,
				},
			},
			expectHasError: true,
		},
		{
			name: "NFS volume already exists with volumeName",
			nfsAPIClient: &fakeNFSAPIClient{
				findResponse: &api.SearchNFSResponse{
					NFS: []sacloud.NFS{
						buildFakeNFS(1, &sacloud.CreateNFSValue{
							Name: "fake",
							Tags: []string{fromCSIMarkerTag},
						}),
					},
				},
			},
			request: &csi.CreateVolumeRequest{
				Name: "fake",
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: 100 * GiB,
					LimitBytes:    100 * GiB,
				},
			},
			expectResponse: &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					Id:            "1",
					CapacityBytes: 100 * GiB,
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fakeDriver.sakuraNFSClient = testCase.nfsAPIClient

			res, err := fakeDriver.CreateVolume(context.Background(), testCase.request)
			if testCase.expectResponse == nil {
				assert.Nil(t, res)
			} else {
				assert.Equal(t, testCase.expectResponse, res)
			}
			if !assert.Equal(t, testCase.expectHasError, err != nil) {
				t.Logf("error => %s\n", err)
			}
		})
	}
}

func buildFakeNFS(id int64, params *sacloud.CreateNFSValue) sacloud.NFS {
	nfs := sacloud.NewNFS(params)
	nfs.Resource = sacloud.NewResource(id)
	return *nfs
}

type fakeNFSAPIClient struct {
	findResponse *api.SearchNFSResponse
	findError    error

	createResponse *sacloud.NFS
	createError    error
}

func (f *fakeNFSAPIClient) Find() (*api.SearchNFSResponse, error) {
	return f.findResponse, f.findError
}

func (f *fakeNFSAPIClient) SetEmpty() {
	// noop
}

func (f *fakeNFSAPIClient) SetNameLike(name string) {
	// noop
}

func (f *fakeNFSAPIClient) Create(value *sacloud.NFS) (*sacloud.NFS, error) {
	return f.createResponse, f.createError
}

func (f *fakeNFSAPIClient) SleepUntilUp(id int64, timeout time.Duration) error {
	// noop
	return nil
}

func (f *fakeNFSAPIClient) SleepUntilDown(id int64, timeout time.Duration) error {
	// noop
	return nil
}

func (f *fakeNFSAPIClient) SleepWhileCopying(id int64, timeout time.Duration, maxRetry int) error {
	// noop
	return nil
}
