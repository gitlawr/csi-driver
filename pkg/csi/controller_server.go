package csi

import (
	"context"
	"fmt"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/longhorn/longhorn-manager/util"
	ctlv1 "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	kv1 "kubevirt.io/client-go/api/v1"
	"kubevirt.io/client-go/kubecli"
)

const (
	timeoutAttachDetach = 120 * time.Second
	tickAttachDetach    = 2 * time.Second

	MinimalVolumeSize = 10 * 1024 * 1024
)

type ControllerServer struct {
	namespace string

	coreClient                ctlv1.Interface
	virtSubresourceRestClient kubecli.KubevirtClient

	caps        []*csi.ControllerServiceCapability
	accessModes []*csi.VolumeCapability_AccessMode
}

func NewControllerServer(coreClient ctlv1.Interface, virtClient kubecli.KubevirtClient, namespace string) *ControllerServer {

	return &ControllerServer{
		coreClient:                coreClient,
		virtSubresourceRestClient: virtClient,
		namespace:                 namespace,
		caps: getControllerServiceCapabilities(
			[]csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
				csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
			}),
		accessModes: getVolumeCapabilityAccessModes(
			[]csi.VolumeCapability_AccessMode_Mode{
				csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			}),
	}
}

func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	logrus.Infof("ControllerServer create volume req: %v", req)
	if err := cs.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		logrus.Errorf("CreateVolume: invalid create volume req: %v", req)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Check request parameters like Name and Volume Capabilities
	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	volumeCaps := req.GetVolumeCapabilities()
	if err := cs.validateVolumeCapabilities(volumeCaps); err != nil {
		return nil, err
	}
	volumeParameters := req.GetParameters()
	if volumeParameters == nil {
		volumeParameters = map[string]string{}
	}

	// snapshot restoring and volume cloning unimplemented
	if req.VolumeContentSource != nil {
		return nil, status.Error(codes.Unimplemented, "")
	}

	// Create a PVC from the host cluster
	volumeMode := corev1.PersistentVolumeBlock
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cs.namespace,
			Name:      req.Name,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: pointer.StringPtr("longhorn"),
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			VolumeMode:       &volumeMode,
		},
	}
	volSizeBytes := int64(MinimalVolumeSize)
	if req.GetCapacityRange() != nil {
		volSizeBytes = req.GetCapacityRange().GetRequiredBytes()
	}
	if volSizeBytes < util.MinimalVolumeSize {
		logrus.Warnf("Request volume %v size %v is smaller than minimal size %v, set it to minimal size.", req.Name, volSizeBytes, util.MinimalVolumeSize)
		volSizeBytes = util.MinimalVolumeSize
	}
	// Round up to multiple of 2 * 1024 * 1024
	volSizeBytes = util.RoundUpSize(volSizeBytes)
	pvc.Spec.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: *resource.NewQuantity(volSizeBytes, resource.BinarySI),
		},
	}

	resPVC, err := cs.coreClient.PersistentVolumeClaim().Create(pvc)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      resPVC.Name,
			CapacityBytes: volSizeBytes,
			VolumeContext: volumeParameters,
			ContentSource: req.VolumeContentSource,
		},
	}, nil
}

func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	logrus.Infof("ControllerServer delete volume req: %v", req)
	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if err := cs.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		logrus.Errorf("DeleteVolume: invalid delete volume req: %v", req)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := cs.coreClient.PersistentVolumeClaim().Delete(cs.namespace, req.GetVolumeId(), &metav1.DeleteOptions{}); errors.IsNotFound(err) {
		return &csi.DeleteVolumeResponse{}, nil
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	logrus.Infoln("ControllerGetCapabilities")
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: cs.caps,
	}, nil
}

func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	logrus.Infof("ControllerServer ValidateVolumeCapabilities req: %v", req)

	_, err := cs.coreClient.PersistentVolumeClaim().Get(cs.namespace, req.GetVolumeId(), metav1.GetOptions{})
	if errors.IsNotFound(err) {
		msg := fmt.Sprintf("ValidateVolumeCapabilities: the PVC %s not exists", req.GetVolumeId())
		logrus.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := cs.validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: req.GetVolumeCapabilities(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

func (cs *ControllerServer) ControllerGetVolume(context.Context, *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	logrus.Infoln("ControllerGetVolume")
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerPublishVolume will attach the volume to the specified node
func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	logrus.Infof("ControllerServer ControllerPublishVolume req: %v", req)

	if req.GetNodeId() == "" {
		msg := "ControllerPublishVolume: missing node id in request"
		logrus.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		msg := fmt.Sprint("ControllerPublishVolume: missing volume capability in request")
		logrus.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	pvc, err := cs.coreClient.PersistentVolumeClaim().Get(cs.namespace, req.GetVolumeId(), metav1.GetOptions{})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		return nil, status.Errorf(codes.Aborted, "The PVC %s in phase %v is not ready to be attached",
			req.GetVolumeId(), pvc.Status.Phase)
	}

	opts := &kv1.AddVolumeOptions{
		Name: req.VolumeId,
		Disk: &kv1.Disk{
			DiskDevice: kv1.DiskDevice{
				Disk: &kv1.DiskTarget{
					// KubeVirt only supports SCSI for hot-plug volumes.
					Bus: "scsi",
				},
			},
		},
		VolumeSource: &kv1.HotplugVolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: req.VolumeId,
			},
		},
	}

	if err := cs.virtSubresourceRestClient.VirtualMachine(cs.namespace).AddVolume(req.GetNodeId(), opts); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	checkPVCBound := func(vol *corev1.PersistentVolumeClaim) bool {
		return vol.Status.Phase == corev1.ClaimBound
	}

	if !cs.waitForPVCState(req.VolumeId, "Bound", checkPVCBound) {
		return nil, status.Errorf(codes.DeadlineExceeded, "Failed to attach volume %s to node %s", req.GetVolumeId(), req.GetNodeId())
	}

	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume will detach the volume
func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	logrus.Infof("ControllerServer ControllerUnpublishVolume req: %v", req)

	if req.GetVolumeId() == "" {
		msg := "ControllerUnpublishVolume: missing volume id in request"
		logrus.Warn(msg)
		return nil, status.Error(codes.InvalidArgument, msg)
	}

	pvc, err := cs.coreClient.PersistentVolumeClaim().Get(cs.namespace, req.GetVolumeId(), metav1.GetOptions{})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		return nil, status.Errorf(codes.Aborted, "The PVC %s is in phase %v",
			req.GetVolumeId(), pvc.Status.Phase)
	}

	opts := &kv1.RemoveVolumeOptions{
		Name: req.VolumeId,
	}
	if err := cs.virtSubresourceRestClient.VirtualMachine(cs.namespace).RemoveVolume(req.GetNodeId(), opts); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *ControllerServer) ListVolumes(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) GetCapacity(context.Context, *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ListSnapshots(context.Context, *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	pvc, err := cs.coreClient.PersistentVolumeClaim().Get(cs.namespace, req.VolumeId, metav1.GetOptions{})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if pvc.Spec.VolumeName == "" {
		msg := fmt.Sprintf("ControllerExpandVolume: the volume %s does not exist", req.GetVolumeId())
		logrus.Warn(msg)
		return nil, status.Errorf(codes.NotFound, msg)
	}

	pvc.Spec.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: *resource.NewQuantity(req.CapacityRange.RequiredBytes, resource.BinarySI),
		},
	}
	if _, err := cs.coreClient.PersistentVolumeClaim().Update(pvc); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	checkPVCExpanded := func(vol *corev1.PersistentVolumeClaim) bool {
		return vol.Status.Capacity.Storage().Value() >= req.CapacityRange.GetRequiredBytes()
	}

	if !cs.waitForPVCState(req.VolumeId, "Expanded", checkPVCExpanded) {
		return nil, status.Errorf(codes.DeadlineExceeded, "Failed to expand volume %s ", req.GetVolumeId())
	}
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         req.CapacityRange.GetRequiredBytes(),
		NodeExpansionRequired: false,
	}, nil
}

func (cs *ControllerServer) validateControllerServiceRequest(c csi.ControllerServiceCapability_RPC_Type) error {
	if c == csi.ControllerServiceCapability_RPC_UNKNOWN {
		return nil
	}

	for _, cap := range cs.caps {
		if c == cap.GetRpc().GetType() {
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, "unsupported capability %s", c)
}

func (cs *ControllerServer) validateVolumeCapabilities(volumeCaps []*csi.VolumeCapability) error {
	if volumeCaps == nil {
		return status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	for _, cap := range volumeCaps {
		if cap.GetMount() == nil && cap.GetBlock() == nil {
			return status.Error(codes.InvalidArgument, "cannot have both mount and block access type be undefined")
		}
		if cap.GetMount() != nil && cap.GetBlock() != nil {
			return status.Error(codes.InvalidArgument, "cannot have both block and mount access type")
		}

		supportedMode := false
		for _, m := range cs.accessModes {
			if cap.GetAccessMode().GetMode() == m.GetMode() {
				supportedMode = true
				break
			}
		}
		if !supportedMode {
			return status.Errorf(codes.InvalidArgument, "access mode %v is not supported", cap.GetAccessMode().Mode.String())
		}
	}

	return nil
}

func getControllerServiceCapabilities(cl []csi.ControllerServiceCapability_RPC_Type) []*csi.ControllerServiceCapability {
	var cscs []*csi.ControllerServiceCapability

	for _, cap := range cl {
		logrus.Infof("Enabling controller service capability: %v", cap.String())
		cscs = append(cscs, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return cscs
}

func getVolumeCapabilityAccessModes(vc []csi.VolumeCapability_AccessMode_Mode) []*csi.VolumeCapability_AccessMode {
	var vca []*csi.VolumeCapability_AccessMode
	for _, c := range vc {
		logrus.Infof("Enabling volume access mode: %v", c.String())
		vca = append(vca, &csi.VolumeCapability_AccessMode{Mode: c})
	}
	return vca
}

func (cs *ControllerServer) waitForPVCState(name string, stateDescription string,
	predicate func(pvc *corev1.PersistentVolumeClaim) bool) bool {
	timer := time.NewTimer(timeoutAttachDetach)
	defer timer.Stop()
	timeout := timer.C

	ticker := time.NewTicker(tickAttachDetach)
	defer ticker.Stop()
	tick := ticker.C

	for {
		select {
		case <-timeout:
			logrus.Warnf("waitForPVCState: timeout while waiting for PVC %s state %s", name, stateDescription)
			return false
		case <-tick:
			logrus.Debugf("Polling PVC %s state for %s at %s", name, stateDescription, time.Now().String())
			existVol, err := cs.coreClient.PersistentVolumeClaim().Get(cs.namespace, name, metav1.GetOptions{})
			if err != nil {
				logrus.Warnf("waitForPVCState: error while waiting for PVC %s state %s error %s", name, stateDescription, err)
				continue
			}
			if predicate(existVol) {
				return true
			}
		}
	}
}
