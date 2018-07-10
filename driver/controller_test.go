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

func TestDriver_DeleteVolume(t *testing.T) {

	t.Run("target volume already deleted", func(t *testing.T) {
		fakeDriver.sakuraNFSClient = &fakeNFSAPIClient{
			readError: api.NewError(404, nil),
		}

		resp, err := fakeDriver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			VolumeId: "1",
		})

		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("error response is not 404", func(t *testing.T) {
		fakeDriver.sakuraNFSClient = &fakeNFSAPIClient{
			readError: api.NewError(400, nil),
		}

		resp, err := fakeDriver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			VolumeId: "1",
		})

		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("no error", func(t *testing.T) {
		fakeNFS := buildFakeNFS(1, &sacloud.CreateNFSValue{
			Name: "fake",
			Tags: []string{fromCSIMarkerTag},
		})

		fakeDriver.sakuraNFSClient = &fakeNFSAPIClient{
			readResponse:   &fakeNFS,
			deleteResponse: &fakeNFS,
		}

		resp, err := fakeDriver.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			VolumeId: "1",
		})

		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})
}

func TestDriver_ControllerPublishVolume(t *testing.T) {

	t.Run("target volume is not exists", func(t *testing.T) {
		fakeDriver.sakuraNFSClient = &fakeNFSAPIClient{
			readError: api.NewError(404, nil),
		}

		resp, err := fakeDriver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
			VolumeId: "1",
		})

		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("target volume is not exists", func(t *testing.T) {
		fakeNFS := buildFakeNFS(1, &sacloud.CreateNFSValue{
			Name:      "fake",
			Tags:      []string{fromCSIMarkerTag},
			IPAddress: "192.2.0.1",
		})

		fakeDriver.sakuraNFSClient = &fakeNFSAPIClient{
			readResponse: &fakeNFS,
		}

		resp, err := fakeDriver.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
			VolumeId: "1",
		})

		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "nfs://192.2.0.1:/export", resp.PublishInfo[NFSPublishInfoURL])
	})
}

func TestDriver_ValidateVolumeCapabilities(t *testing.T) {

	expects := map[csi.VolumeCapability_AccessMode_Mode]bool{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER:       false,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY:  false,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY:   false,
		csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER: false,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:  true,
	}

	for cap, expect := range expects {

		req := &csi.ValidateVolumeCapabilitiesRequest{
			VolumeCapabilities: []*csi.VolumeCapability{
				{
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: cap,
					},
				},
			},
		}

		resp, err := fakeDriver.ValidateVolumeCapabilities(context.Background(), req)

		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, expect, resp.Supported)
	}

}

type fakeNFSAPIClient struct {
	findResponse *api.SearchNFSResponse
	findError    error

	readResponse *sacloud.NFS
	readError    error

	createResponse *sacloud.NFS
	createError    error

	deleteResponse *sacloud.NFS
	deleteError    error

	stopError error
	waitError error
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

func (f *fakeNFSAPIClient) Read(id int64) (*sacloud.NFS, error) {
	return f.readResponse, f.readError
}

func (f *fakeNFSAPIClient) Create(value *sacloud.NFS) (*sacloud.NFS, error) {
	return f.createResponse, f.createError
}

func (f *fakeNFSAPIClient) Stop(id int64) (bool, error) {
	return true, f.stopError
}

func (f *fakeNFSAPIClient) Delete(id int64) (*sacloud.NFS, error) {
	return f.deleteResponse, f.deleteError
}

func (f *fakeNFSAPIClient) SleepUntilUp(id int64, timeout time.Duration) error {
	return f.waitError
}

func (f *fakeNFSAPIClient) SleepUntilDown(id int64, timeout time.Duration) error {
	return f.waitError
}

func (f *fakeNFSAPIClient) SleepWhileCopying(id int64, timeout time.Duration, maxRetry int) error {
	return f.waitError
}

func buildFakeNFS(id int64, params *sacloud.CreateNFSValue) sacloud.NFS {
	nfs := sacloud.NewNFS(params)
	nfs.Resource = sacloud.NewResource(id)
	return *nfs
}
