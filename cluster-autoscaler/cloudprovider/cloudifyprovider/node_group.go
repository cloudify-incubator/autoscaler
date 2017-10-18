/*
Copyright (c) 2017 GigaSpaces Technologies Ltd. All rights reserved

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cloudifyprovider

import (
	cloudify "github.com/cloudify-incubator/cloudify-rest-go-client/cloudify"
	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
)

type CloudifyNodeGroup struct {
	client       *cloudify.CloudifyClient
	nodeID       string
	deploymentID string
}

// MaxSize returns maximum size of the node group.
func (clng *CloudifyNodeGroup) MaxSize() int {
	glog.Warningf("MaxSize")
	return 0
}

// MinSize returns minimum size of the node group.
func (clng *CloudifyNodeGroup) MinSize() int {
	glog.Warningf("MinSize")
	return 0
}

// TargetSize returns the current target size of the node group. It is possible that the
// number of nodes in Kubernetes is different at the moment but should be equal
// to Size() once everything stabilizes (new nodes finish startup and registration or
// removed nodes are deleted completely). Implementation required.
func (clng *CloudifyNodeGroup) TargetSize() (int, error) {
	glog.Warningf("TargetSize")
	return 0, cloudprovider.ErrNotImplemented
}

// IncreaseSize increases the size of the node group. To delete a node you need
// to explicitly name it and use DeleteNode. This function should wait until
// node group size is updated. Implementation required.
func (clng *CloudifyNodeGroup) IncreaseSize(delta int) error {
	glog.Warningf("IncreaseSize: %v", delta)
	return cloudprovider.ErrNotImplemented
}

// DeleteNodes deletes nodes from this node group. Error is returned either on
// failure or if the given node doesn't belong to this node group. This function
// should wait until node group size is updated. Implementation required.
func (clng *CloudifyNodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	glog.Warningf("DeleteNodes: %+v", nodes)
	return cloudprovider.ErrNotImplemented
}

// DecreaseTargetSize decreases the target size of the node group. This function
// doesn't permit to delete any existing node and can be used only to reduce the
// request for new nodes that have not been yet fulfilled. Delta should be negative.
// It is assumed that cloud provider will not delete the existing nodes when there
// is an option to just decrease the target. Implementation required.
func (clng *CloudifyNodeGroup) DecreaseTargetSize(delta int) error {
	glog.Warningf("DecreaseTargetSize: %v", delta)
	return cloudprovider.ErrNotImplemented
}

// Id returns an unique identifier of the node group.
func (clng *CloudifyNodeGroup) Id() string {
	glog.Warningf("Id: %+v", clng.nodeID)
	return clng.nodeID
}

// Debug returns a string containing all information regarding this node group.
func (clng *CloudifyNodeGroup) Debug() string {
	glog.Warningf("Debug")
	return ""
}

// Nodes returns a list of all nodes that belong to this node group.
func (clng *CloudifyNodeGroup) Nodes() ([]string, error) {
	glog.Warningf("Nodes")
	return []string{}, cloudprovider.ErrNotImplemented
}

// TemplateNodeInfo returns a schedulercache.NodeInfo structure of an empty
// (as if just started) node. This will be used in scale-up simulations to
// predict what would a new node look like if a node group was expanded. The returned
// NodeInfo is expected to have a fully populated Node object, with all of the labels,
// capacity and allocatable information as well as all pods that are started on
// the node by default, using manifest (most likely only kube-proxy). Implementation optional.
func (clng *CloudifyNodeGroup) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
	glog.Warningf("TemplateNodeInfo")
	return nil, cloudprovider.ErrNotImplemented
}

// Exist checks if the node group really exists on the cloud provider side. Allows to tell the
// theoretical node group from the real one. Implementation required.
func (clng *CloudifyNodeGroup) Exist() bool {
	glog.Warningf("Exist")
	return false
}

// Create creates the node group on the cloud provider side. Implementation optional.
func (clng *CloudifyNodeGroup) Create() error {
	glog.Warningf("Create")
	return cloudprovider.ErrNotImplemented
}

// Delete deletes the node group on the cloud provider side.
// This will be executed only for autoprovisioned node groups, once their size drops to 0.
// Implementation optional.
func (clng *CloudifyNodeGroup) Delete() error {
	glog.Warningf("Delete")
	return cloudprovider.ErrNotImplemented
}

// Autoprovisioned returns true if the node group is autoprovisioned. An autoprovisioned group
// was created by CA and can be deleted when scaled to 0.
func (clng *CloudifyNodeGroup) Autoprovisioned() bool {
	glog.Warningf("Autoprovisioned")
	return false
}

func CreateNodeGroup(client *cloudify.CloudifyClient, deployment, group_name string) *CloudifyNodeGroup {
	glog.Warningf("CreateNodeGroup")
	return &CloudifyNodeGroup{
		client:       client,
		nodeID:       group_name,
		deploymentID: deployment,
	}
}
