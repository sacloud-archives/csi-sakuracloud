package driver

import (
	"context"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
)

// CreateVolume creates a new volume from the given request
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	// TODO not implements
	return nil, nil
}

// DeleteVolume deletes the given volume
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	// TODO not implements
	return nil, nil
}

// ControllerPublishVolume attaches the given volume to the node
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	// TODO not implements
	return nil, nil
}

// ControllerUnpublishVolume detaches the given volume from the node
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	// TODO not implements
	return nil, nil
}

// ValidateVolumeCapabilities checks whether the volume capabilities requested
// are supported.
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	// TODO not implements
	return nil, nil
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
