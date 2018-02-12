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
	"fmt"
	cloudify "github.com/cloudify-incubator/cloudify-rest-go-client/cloudify"
	utils "github.com/cloudify-incubator/cloudify-rest-go-client/cloudify/utils"
	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
	"math/rand"
)

// NodeGroup - settings for connect to cloudify
type NodeGroup struct {
	client       *cloudify.Client
	scaleGroup   string
	deploymentID string
}

// MaxSize returns maximum size of the node group.
func (clng *NodeGroup) MaxSize() int {
	glog.V(4).Infof("MaxSize(%v.%v)", clng.deploymentID, clng.scaleGroup)
	scaleGroup, err := clng.client.GetDeploymentScaleGroup(clng.deploymentID, clng.scaleGroup)
	if err != nil {
		glog.Errorf("Issues with get limits:%+v", clng.scaleGroup)
		return 0
	}
	var size = scaleGroup.Properties.MaxInstances
	if size < 0 {
		// unlimited is 100 nodes, by default
		size = 100
	}
	glog.Warningf("MaxSize(%v.%v):%+v", clng.deploymentID, clng.scaleGroup, size)
	return size
}

// MinSize returns minimum size of the node group.
func (clng *NodeGroup) MinSize() int {
	glog.V(4).Infof("MinSize(%v.%v)", clng.deploymentID, clng.scaleGroup)
	scaleGroup, err := clng.client.GetDeploymentScaleGroup(clng.deploymentID, clng.scaleGroup)
	if err != nil {
		glog.Errorf("Issues with get limits:%+v", clng.scaleGroup)
		return 0
	}
	var size = scaleGroup.Properties.MinInstances
	glog.Warningf("MinSize(%v.%v):%+v", clng.deploymentID, clng.scaleGroup, size)
	return size
}

// TargetSize returns the current target size of the node group. It is possible that the
// number of nodes in Kubernetes is different at the moment but should be equal
// to Size() once everything stabilizes (new nodes finish startup and registration or
// removed nodes are deleted completely). Implementation required.
func (clng *NodeGroup) TargetSize() (int, error) {
	glog.V(4).Infof("TargetSize(%v.%v)", clng.deploymentID, clng.scaleGroup)
	scaleGroup, err := clng.client.GetDeploymentScaleGroup(clng.deploymentID, clng.scaleGroup)
	if err != nil {
		return 0, fmt.Errorf("Issues with get limits:%+v", clng.scaleGroup)
	}
	var size = scaleGroup.Properties.PlannedInstances
	glog.Warningf("TargetSize(%v.%v):%+v", clng.deploymentID, clng.scaleGroup, size)
	return size, nil
}

// IncreaseSize increases the size of the node group. To delete a node you need
// to explicitly name it and use DeleteNode. This function should wait until
// node group size is updated. Implementation required.
func (clng *NodeGroup) IncreaseSize(delta int) error {
	glog.V(4).Infof("IncreaseSize(%v.%v): %v", clng.deploymentID, clng.scaleGroup, delta)

	scaleGroup, err := clng.client.GetDeploymentScaleGroup(clng.deploymentID, clng.scaleGroup)
	if err != nil {
		return fmt.Errorf("Issues with get limits:%+v", clng.scaleGroup)
	}

	// Check for finish executuons
	err = clng.client.WaitBeforeRunExecution(clng.deploymentID)
	if err != nil {
		return err
	}

	if scaleGroup.Properties.MaxInstances < 0 ||
		scaleGroup.Properties.CurrentInstances < scaleGroup.Properties.MaxInstances {
		var exec cloudify.ExecutionPost
		exec.WorkflowID = "scale"
		exec.DeploymentID = clng.deploymentID
		exec.Parameters = map[string]interface{}{}
		exec.Parameters["scalable_entity_name"] = clng.scaleGroup // Use scale group instead real node id
		execution, err := clng.client.RunExecution(exec, false)
		if err != nil {
			return err
		}
		glog.Warningf("Final status for %v, last status: %v", execution.ID, execution.Status)
		if execution.Status == "failed" {
			return fmt.Errorf(execution.ErrorMessage)
		}
		return nil
	}
	glog.Warningf("No place to scale(%v.%v)", clng.deploymentID, clng.scaleGroup)
	return fmt.Errorf("No place to scale")
}

// DeleteNodes deletes nodes from this node group. Error is returned either on
// failure or if the given node doesn't belong to this node group. This function
// should wait until node group size is updated. Implementation required.
func (clng *NodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	glog.V(4).Infof("DeleteNodes: (%v.%v): %+v", clng.deploymentID, clng.scaleGroup, nodes)
	var removedIdsIncludeHint = []string{}

	// Check for finish executuons
	err := clng.client.WaitBeforeRunExecution(clng.deploymentID)
	if err != nil {
		return err
	}
	// Check for finish executuons
	err = clng.client.WaitBeforeRunExecution(clng.deploymentID)
	if err != nil {
		return err
	}

	// filter nodes
	params := map[string]string{}
	params["deployment_id"] = clng.deploymentID
	cloudInstances, err := clng.client.GetStartedNodeInstancesWithType(
		params, "cloudify.nodes.ApplicationServer.kubernetes.Node")
	if err != nil {
		return err
	}
	for _, cloudInstance := range cloudInstances.Items {
		for _, kubeNode := range nodes {
			hostName := cloudInstance.GetStringProperty("hostname")
			// check runtime properties
			if hostName != kubeNode.Name {
				// node with different name
				continue
			}
			removedIdsIncludeHint = append(removedIdsIncludeHint, cloudInstance.ID)
		}
	}

	var removedIdsExcludeHint = []string{}
	instanceNames, err := clng.InstancesNames()
	if err != nil {
		return err
	}
	for _, instanceName := range instanceNames {
		if !utils.InList(removedIdsIncludeHint, instanceName) {
			removedIdsExcludeHint = append(removedIdsExcludeHint, instanceName)
		}
	}

	glog.Warningf("Node about to delete: (%v.%v): -%v +%v",
		clng.deploymentID, clng.scaleGroup,
		removedIdsIncludeHint, removedIdsExcludeHint)

	var exec cloudify.ExecutionPost
	exec.WorkflowID = "delete"
	exec.DeploymentID = clng.deploymentID
	exec.Parameters = map[string]interface{}{}
	exec.Parameters["scalable_entity_name"] = clng.scaleGroup // Use scale group instead real node id
	exec.Parameters["removed_ids_include_hint"] = removedIdsIncludeHint
	exec.Parameters["removed_ids_exclude_hint"] = removedIdsExcludeHint
	exec.Parameters["delta"] = -len(removedIdsIncludeHint)
	execution, err := clng.client.RunExecution(exec, false)
	if err != nil {
		return err
	}
	glog.Warningf("Final status for %v, last status: %v", execution.ID, execution.Status)
	if execution.Status == "failed" {
		return fmt.Errorf(execution.ErrorMessage)
	}

	return nil
}

// DecreaseTargetSize decreases the target size of the node group. This function
// doesn't permit to delete any existing node and can be used only to reduce the
// request for new nodes that have not been yet fulfilled. Delta should be negative.
// It is assumed that cloud provider will not delete the existing nodes when there
// is an option to just decrease the target. Implementation required.
func (clng *NodeGroup) DecreaseTargetSize(delta int) error {
	glog.Errorf("?DecreaseTargetSize: %v", delta)
	return cloudprovider.ErrNotImplemented
}

// Id returns an unique identifier of the node group.
func (clng *NodeGroup) Id() string {
	return clng.deploymentID + "." + clng.scaleGroup
}

// Debug returns a string containing all information regarding this node group.
func (clng *NodeGroup) Debug() string {
	glog.Errorf("?Debug")
	return ""
}

// Nodes returns a list of all nodes that belong to this node group.
func (clng *NodeGroup) Nodes() ([]string, error) {
	glog.V(4).Infof("Nodes(%v.%v)", clng.deploymentID, clng.scaleGroup)

	nodeInstancesList := []string{}
	nodeInstances, err := clng.client.GetDeploymentScaleGroupInstances(
		clng.deploymentID, clng.scaleGroup,
		"cloudify.nodes.ApplicationServer.kubernetes.Node")
	if err != nil {
		glog.Errorf("Issues with get scale group%+v", clng.scaleGroup)
		return nodeInstancesList, err
	}
	for _, instance := range nodeInstances.Items {
		hostName := instance.GetStringProperty("hostname")

		if hostName != "" {
			nodeInstancesList = append(nodeInstancesList, hostName)
		}
	}
	glog.Warningf("Nodes(%v.%v): %+v", clng.deploymentID, clng.scaleGroup, nodeInstancesList)
	return nodeInstancesList, nil
}

// InstancesNames - returns a list of all nodes that belong to this node group.
func (clng *NodeGroup) InstancesNames() ([]string, error) {
	glog.V(4).Infof("InstancesNames(%v.%v)", clng.deploymentID, clng.scaleGroup)

	nodeInstancesList := []string{}
	nodeInstances, err := clng.client.GetDeploymentScaleGroupInstances(
		clng.deploymentID, clng.scaleGroup,
		"cloudify.nodes.ApplicationServer.kubernetes.Node")
	if err != nil {
		glog.Errorf("Issues with get scale group%+v", clng.scaleGroup)
		return nodeInstancesList, err
	}
	for _, instance := range nodeInstances.Items {
		nodeInstancesList = append(nodeInstancesList, instance.ID)
	}
	glog.Warningf("InstancesNames(%v.%v): %+v", clng.deploymentID, clng.scaleGroup, nodeInstancesList)
	return nodeInstancesList, nil
}

// getCurrentCharacteristics - return characteristics for vm in scaling group
func (clng *NodeGroup) getCurrentCharacteristics() (int64, int64) {
	var cpu int64 = 1
	var memory int64 = 512
	nodes, err := clng.client.GetDeploymentScaleGroupNodes(clng.deploymentID, clng.scaleGroup,
		"cloudify.nodes.ApplicationServer.kubernetes.Node")
	if err != nil {
		glog.Errorf("Issues with get scale group%+v", clng.scaleGroup)
		return cpu, memory
	}
	for _, cloudNode := range nodes.Items {
		if cloudNode.Properties != nil {
			if v, ok := cloudNode.Properties["kubemem"]; ok == true {
				switch v.(type) {
				case int:
				case uint8:
				case uint16:
				case uint32:
				case uint64:
				case int8:
				case int16:
				case int32:
				case int64:
					{
						if memory < int64(v.(int)) {
							memory = int64(v.(int))
						}
					}
				}
			}
			if v, ok := cloudNode.Properties["kubecpu"]; ok == true {
				switch v.(type) {
				case int:
				case uint8:
				case uint16:
				case uint32:
				case uint64:
				case int8:
				case int16:
				case int32:
				case int64:
					{
						if cpu < int64(v.(int)) {
							cpu = int64(v.(int))
						}
					}
				}
			}
		}
	}
	glog.Warningf("getCurrentCharacteristics(%v.%v):cpu %v, mem: %v", clng.deploymentID, clng.scaleGroup, cpu, memory)
	return cpu, memory
}

// TemplateNodeInfo returns a schedulercache.NodeInfo structure of an empty
// (as if just started) node. This will be used in scale-up simulations to
// predict what would a new node look like if a node group was expanded. The returned
// NodeInfo is expected to have a fully populated Node object, with all of the labels,
// capacity and allocatable information as well as all pods that are started on
// the node by default, using manifest (most likely only kube-proxy). Implementation optional.
func (clng *NodeGroup) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
	glog.V(4).Infof("TemplateNodeInfo(%v.%v)", clng.deploymentID, clng.scaleGroup)

	node := apiv1.Node{}
	nodeName := fmt.Sprintf("%s-cloudify-%d", clng.Id(), rand.Int63())

	node.ObjectMeta = metav1.ObjectMeta{
		Name:     nodeName,
		SelfLink: fmt.Sprintf("/api/v1/nodes/%s", nodeName),
		Labels:   map[string]string{},
	}

	node.Status = apiv1.NodeStatus{
		Capacity: apiv1.ResourceList{},
	}

	// TODO: get a real value.
	cpu, memory := clng.getCurrentCharacteristics()

	node.Status.Capacity[apiv1.ResourcePods] = *resource.NewQuantity(110, resource.DecimalSI)
	node.Status.Capacity[apiv1.ResourceCPU] = *resource.NewQuantity(cpu, resource.DecimalSI)
	node.Status.Capacity[apiv1.ResourceNvidiaGPU] = *resource.NewQuantity(0, resource.DecimalSI)
	node.Status.Capacity[apiv1.ResourceMemory] = *resource.NewQuantity(memory*1024*1024, resource.DecimalSI)

	// TODO: use proper allocatable!!
	node.Status.Allocatable = node.Status.Capacity

	node.Status.Conditions = cloudprovider.BuildReadyConditions()

	nodeInfo := schedulercache.NewNodeInfo(cloudprovider.BuildKubeProxy(clng.Id()))
	nodeInfo.SetNode(&node)

	glog.Warningf("Returned templateNodeInfo(%v.%v):%+v", clng.deploymentID, clng.scaleGroup, nodeInfo)
	return nodeInfo, nil
}

// Exist checks if the node group really exists on the cloud provider side. Allows to tell the
// theoretical node group from the real one. Implementation required.
func (clng *NodeGroup) Exist() bool {
	glog.V(4).Infof("Exist(%v.%v)", clng.deploymentID, clng.scaleGroup)
	_, err := clng.client.GetDeploymentScaleGroup(clng.deploymentID, clng.scaleGroup)
	if err != nil {
		glog.Errorf("Issues with get limits:%+v", clng.scaleGroup)
		return false
	}
	return true
}

// Create creates the node group on the cloud provider side. Implementation optional.
func (clng *NodeGroup) Create() error {
	glog.Errorf("?Create")
	return cloudprovider.ErrNotImplemented
}

// Delete deletes the node group on the cloud provider side.
// This will be executed only for autoprovisioned node groups, once their size drops to 0.
// Implementation optional.
func (clng *NodeGroup) Delete() error {
	glog.Errorf("?Delete")
	return cloudprovider.ErrNotImplemented
}

// Autoprovisioned returns true if the node group is autoprovisioned. An autoprovisioned group
// was created by CA and can be deleted when scaled to 0.
func (clng *NodeGroup) Autoprovisioned() bool {
	glog.Errorf("?Autoprovisioned")
	return false
}

// String return scaleGroup name
func (clng *NodeGroup) String() string {
	return clng.scaleGroup
}

// NodeToNodeGroup - create cloudify node group
func NodeToNodeGroup(client *cloudify.Client, deploymentID, groupName string) *NodeGroup {
	glog.V(4).Infof("NodeToNodeGroup(%v.%v)", deploymentID, groupName)
	return &NodeGroup{
		client:       client,
		scaleGroup:   groupName,
		deploymentID: deploymentID,
	}
}
