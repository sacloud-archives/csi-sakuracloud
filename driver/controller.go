package driver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/sacloud/libsacloud/api"
	"github.com/sacloud/libsacloud/sacloud"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	GiB                  = 1024 * 1024 * 1024
	defaultVolumeSizeGiB = 100
	fromCSIMarkerTag     = "@csi-sakuracloud"
)

const (
	NFSParameterKeySwitchID       = "switchID"
	NFSParameterKeyIPAddress      = "ipaddress"
	NFSParameterKeyDefaultGateway = "defaultGateway"
	NFSParameterKeyDescription    = "description"
	NFSPublishInfoURL             = "url"
)

// CreateVolume creates a new volume from the given request
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume: Name must be provided")
	}

	if req.VolumeCapabilities == nil || len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Volume capabilities must be provided")
	}

	size, err := extractStorage(req.CapacityRange)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	volumeName := sanitizeNFSName(req.Name)

	ll := d.log.WithFields(logrus.Fields{
		"volume_name":             volumeName,
		"storage_size_giga_bytes": size / GiB,
		"method":                  "create_volume",
	})
	ll.Info("create volume called")

	volumes, err := d.findNFSByName(volumeName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if volumes != nil && len(volumes) > 0 {
		if len(volumes) > 1 {
			return nil, fmt.Errorf("fatal issue: duplicate volume %q exists", volumeName)
		}
		vol := volumes[0]

		if !vol.HasTag(fromCSIMarkerTag) {
			return nil, fmt.Errorf("fatal issue: volume %q (%s) was not created by CSI",
				vol.Name, vol.Description)
		}

		// note: SakuraCloud's NFS appliance has size(GiB) into Plan.ID
		if vol.Plan.ID != size {
			return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("invalid option requested size: %d", size))
		}

		ll.Info("volume already created")
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				Id:            vol.GetStrID(),
				CapacityBytes: vol.Plan.ID * GiB,
			},
		}, nil
	}

	// create NFS
	volumeReq, err := buildNFSVolumeParam(volumeName, size, req.Parameters)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ll.WithField("volume_req", volumeReq).Info("creating volume")

	nfs, err := d.sakuraNFSClient.Create(
		sacloud.NewNFS(volumeReq),
	)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// wait for available
	// TODO timeout and max retry count are should make configurable
	if err := d.sakuraNFSClient.SleepWhileCopying(nfs.ID, time.Hour, 100); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := d.sakuraNFSClient.SleepUntilUp(nfs.ID, time.Hour); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			Id:            nfs.GetStrID(),
			CapacityBytes: nfs.Plan.ID * GiB,
		},
	}

	ll.WithField("response", resp).Info("volume created")
	return resp, nil

	return nil, nil
}

// DeleteVolume deletes the given volume
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "DeleteVolume Volume ID must be provided")
	}

	ll := d.log.WithFields(logrus.Fields{
		"volume_id": req.VolumeId,
		"method":    "delete_volume",
	})
	ll.Info("delete volume called")

	// read
	resp, err := d.sakuraNFSClient.Read(toSakuraID(req.VolumeId))
	if err != nil {
		if e, ok := err.(api.Error); ok && e.ResponseCode() == http.StatusNotFound {
			// already deleted
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	// stop
	if _, err := d.sakuraNFSClient.Stop(resp.ID); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// wait for stopped
	if _, err := d.sakuraNFSClient.Stop(resp.ID); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// delete
	resp, err = d.sakuraNFSClient.Delete(resp.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	ll.WithField("response", resp).Info("volume is deleted")
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume attaches the given volume to the node
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume Volume ID must be provided")
	}

	// TODO should use VolumeCapability??

	// read
	resp, err := d.sakuraNFSClient.Read(toSakuraID(req.VolumeId))
	if err != nil {
		return nil, err
	}

	addr := resp.Remark.Servers[0].(map[string]interface{})["IPAddress"].(string)
	return &csi.ControllerPublishVolumeResponse{
		PublishInfo: map[string]string{
			NFSPublishInfoURL: fmt.Sprintf("nfs://%s:/export", addr),
		},
	}, nil

	return nil, nil
}

// ControllerUnpublishVolume detaches the given volume from the node
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	// codes.Unimplemented inclused not supported/enabled
	return nil, status.Error(codes.Unimplemented, "")
}

// ValidateVolumeCapabilities checks whether the volume capabilities requested
// are supported.
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	messages := make([]string, 0)
	for _, cap := range req.VolumeCapabilities {
		if cap.AccessMode.Mode != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
			messages = append(messages, fmt.Sprintf("AccessMode %q is not supported", cap.AccessMode.Mode))
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Supported: len(messages) == 0,
		Message:   strings.Join(messages, "\n"),
	}, nil
}

// ListVolumes returns a list of all requested volumes
func (d *Driver) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	// TODO not implements
	return nil, nil
}

// GetCapacity returns the capacity of the storage pool
func (d *Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	// TODO not implements
	return nil, nil
}

// ControllerGetCapabilities returns the capabilities of the controller service.
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	// TODO not implements
	return nil, nil
}

// CreateSnapshot creates a new snapshot from the given request
func (d *Driver) CreateSnapshot(context.Context, *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	// TODO not implements
	return nil, nil
}

// DeleteSnapshot deletes the given snapshot
func (d *Driver) DeleteSnapshot(context.Context, *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	// TODO not implements
	return nil, nil
}

// ListSnapshots returns a list of all requested snapshots
func (d *Driver) ListSnapshots(context.Context, *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	// TODO not implements
	return nil, nil
}

func (d *Driver) findNFSByName(volumeName string) ([]*sacloud.NFS, error) {
	nfsAPI := d.sakuraNFSClient

	nfsAPI.SetEmpty()
	nfsAPI.SetNameLike(volumeName)
	results, err := nfsAPI.Find()
	if err != nil {
		return nil, err
	}

	res := make([]*sacloud.NFS, 0)
	for _, nfs := range results.NFS {
		if nfs.Name == volumeName {
			res = append(res, &nfs)
		}
	}
	return res, nil
}

func sanitizeNFSName(volumeName string) string {
	return strings.Replace(volumeName, "_", "-", -1)
}

// extractStorage extracts the storage size in GB from the given capacity
// range. If the capacity range is not satisfied it returns the default volume
// size.
func extractStorage(capRange *csi.CapacityRange) (int64, error) {
	if capRange == nil {
		return defaultVolumeSizeGiB, nil
	}

	if capRange.RequiredBytes == 0 && capRange.LimitBytes == 0 {
		return defaultVolumeSizeGiB, nil
	}

	minSize := capRange.RequiredBytes

	// limitBytes might be zero
	maxSize := capRange.LimitBytes
	if capRange.LimitBytes == 0 {
		maxSize = minSize
	}

	allowSizes := sacloud.AllowNFSPlans() // unit: GiB
	for _, byteSize := range allowSizes {
		size := int64(byteSize)
		if minSize/GiB <= size && size <= maxSize/GiB {
			return size, nil
		}
	}

	return 0, errors.New("there is no NFS plan that satisfies both of RequiredBytes and LimitBytes")
}

func buildNFSVolumeParam(name string, size int64, params map[string]string) (*sacloud.CreateNFSValue, error) {

	// TODO validate params

	p := sacloud.NewCreateNFSValue()
	p.Name = name
	p.Plan = sacloud.NFSPlan(size)

	mapping := map[string]*string{
		NFSParameterKeySwitchID:       &p.SwitchID,
		NFSParameterKeyIPAddress:      &p.IPAddress,
		NFSParameterKeyDefaultGateway: &p.DefaultRoute,
		NFSParameterKeyDescription:    &p.Description,
	}
	for key, dest := range mapping {
		if v, ok := params[key]; ok {
			*dest = v
		}
	}

	if strMaskLen, ok := params["networkMaskLen"]; ok {
		maskLen, err := strconv.Atoi(strMaskLen)
		if err != nil {
			return nil, err
		}
		p.MaskLen = maskLen
	}

	return p, nil
}
