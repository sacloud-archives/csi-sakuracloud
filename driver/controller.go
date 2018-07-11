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
	var page int
	var err error
	if req.StartingToken != "" {
		page, err = strconv.Atoi(req.StartingToken)
		if err != nil {
			return nil, err
		}
	}

	d.sakuraNFSClient.SetEmpty()
	d.sakuraNFSClient.SetOffset(page)
	d.sakuraNFSClient.SetLimit(int(req.MaxEntries))

	ll := d.log.WithFields(logrus.Fields{
		"req_starting_token": req.StartingToken,
		"method":             "list_volumes",
	})
	ll.Info("list volumes called")

	var volumes []sacloud.NFS
	vols, err := d.sakuraNFSClient.Find()
	if err != nil {
		if e, ok := err.(api.Error); ok && e.ResponseCode() == http.StatusNotFound {
			return &csi.ListVolumesResponse{
				Entries: []*csi.ListVolumesResponse_Entry{},
			}, nil
		}
		return nil, err
	}

	for _, vol := range vols.NFS {
		if vol.HasTag(fromCSIMarkerTag) {
			volumes = append(volumes, vol)
		}
	}

	var entries []*csi.ListVolumesResponse_Entry
	for _, vol := range volumes {
		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				Id:            vol.GetStrID(),
				CapacityBytes: vol.Plan.ID * GiB,
			},
		})
	}

	lastPage := page + 1
	resp := &csi.ListVolumesResponse{
		Entries:   entries,
		NextToken: strconv.Itoa(lastPage),
	}

	ll.WithField("response", resp).Info("volumes listed")
	return resp, nil
}

// GetCapacity returns the capacity of the storage pool
func (d *Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	// TODO not implements
	d.log.WithFields(logrus.Fields{
		"params": req.Parameters,
		"method": "get_capacity",
	}).Warn("get capacity is not implemented")
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerGetCapabilities returns the capabilities of the controller service.
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	newCap := func(cap csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
		return &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
	}

	var caps []*csi.ControllerServiceCapability
	for _, cap := range []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		//csi.ControllerServiceCapability_RPC_GET_CAPACITY,
		//csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		//csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
	} {
		caps = append(caps, newCap(cap))
	}

	resp := &csi.ControllerGetCapabilitiesResponse{
		Capabilities: caps,
	}

	d.log.WithFields(logrus.Fields{
		"response": resp,
		"method":   "controller_get_capabilities",
	}).Info("controller get capabilities called")
	return resp, nil
}

// CreateSnapshot creates a new snapshot from the given request
func (d *Driver) CreateSnapshot(context.Context, *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// DeleteSnapshot deletes the given snapshot
func (d *Driver) DeleteSnapshot(context.Context, *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ListSnapshots returns a list of all requested snapshots
func (d *Driver) ListSnapshots(context.Context, *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
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
