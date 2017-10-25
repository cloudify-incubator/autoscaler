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
	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
	"math/rand"
)

type CloudifyNodeGroup struct {
	client       *cloudify.CloudifyClient
	nodeID       string
	deploymentID string
}

func (clng *CloudifyNodeGroup) getCloudifyNodes() []cloudify.CloudifyNode {
	resulted_nodes := []cloudify.CloudifyNode{}
	for _, cloud_node := range GetCloudifyNodes(clng.client, clng.deploymentID) {
		if cloud_node.Properties != nil {
			// Check tag
			if v, ok := cloud_node.Properties["kubetag"]; ok == true {
				switch v.(type) {
				case string:
					{
						if v.(string) == clng.nodeID {
							resulted_nodes = append(resulted_nodes, cloud_node)
						}
					}
				}
			}
		}
	}
	return resulted_nodes
}

// MaxSize returns maximum size of the node group.
func (clng *CloudifyNodeGroup) MaxSize() int {
	glog.Warningf("MaxSize(%v.%v)", clng.deploymentID, clng.nodeID)
	var size int = 0
	for _, cloud_node := range clng.getCloudifyNodes() {
		max_size := cloud_node.MaxNumberOfInstances
		if max_size < 0 {
			// unlimited is 100 nodes, by default
			max_size = 100
		}
		size += max_size
	}
	glog.Warningf("MaxSize(%v.%v):%+v", clng.deploymentID, clng.nodeID, size)
	return size
}

// MinSize returns minimum size of the node group.
func (clng *CloudifyNodeGroup) MinSize() int {
	glog.Warningf("MinSize(%v.%v)", clng.deploymentID, clng.nodeID)
	var size int = 0
	for _, cloud_node := range clng.getCloudifyNodes() {
		size += cloud_node.MinNumberOfInstances
	}
	glog.Warningf("MinSize(%v.%v):%+v", clng.deploymentID, clng.nodeID, size)
	return size
}

// TargetSize returns the current target size of the node group. It is possible that the
// number of nodes in Kubernetes is different at the moment but should be equal
// to Size() once everything stabilizes (new nodes finish startup and registration or
// removed nodes are deleted completely). Implementation required.
func (clng *CloudifyNodeGroup) TargetSize() (int, error) {
	glog.Warningf("TargetSize(%v.%v)", clng.deploymentID, clng.nodeID)
	var size int = 0
	for _, cloud_node := range clng.getCloudifyNodes() {
		size += cloud_node.PlannedNumberOfInstances
	}
	glog.Warningf("TargetSize(%v.%v):%+v", clng.deploymentID, clng.nodeID, size)
	return size, nil
}

// IncreaseSize increases the size of the node group. To delete a node you need
// to explicitly name it and use DeleteNode. This function should wait until
// node group size is updated. Implementation required.
func (clng *CloudifyNodeGroup) IncreaseSize(delta int) error {
	glog.Warningf("IncreaseSize(%v.%v): %v", clng.deploymentID, clng.nodeID, delta)

	// Check for finish executuons
	err := clng.client.WaitBeforeRunExecution(clng.deploymentID)
	if err != nil {
		return err
	}

	for _, cloud_node := range clng.getCloudifyNodes() {
		if cloud_node.MaxNumberOfInstances < 0 || cloud_node.NumberOfInstances < cloud_node.MaxNumberOfInstances {
			var exec cloudify.CloudifyExecutionPost
			exec.WorkflowId = "scale"
			exec.DeploymentId = clng.deploymentID
			exec.Parameters = map[string]interface{}{}
			exec.Parameters["scalable_entity_name"] = cloud_node.Id
			execution := clng.client.RunExecution(exec, false)
			glog.Warningf("Final status for %v, last status: %v", execution.Id, execution.Status)
			if execution.Status == "failed" {
				return fmt.Errorf(execution.ErrorMessage)
			}
			return nil
		}
	}
	glog.Warningf("No place to scale(%v.%v)", clng.deploymentID, clng.nodeID)
	return fmt.Errorf("No place to scale")
}

// DeleteNodes deletes nodes from this node group. Error is returned either on
// failure or if the given node doesn't belong to this node group. This function
// should wait until node group size is updated. Implementation required.
func (clng *CloudifyNodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	glog.Warningf("?DeleteNodes: %+v", nodes)
	return cloudprovider.ErrNotImplemented
}

// DecreaseTargetSize decreases the target size of the node group. This function
// doesn't permit to delete any existing node and can be used only to reduce the
// request for new nodes that have not been yet fulfilled. Delta should be negative.
// It is assumed that cloud provider will not delete the existing nodes when there
// is an option to just decrease the target. Implementation required.
func (clng *CloudifyNodeGroup) DecreaseTargetSize(delta int) error {
	glog.Warningf("?DecreaseTargetSize: %v", delta)
	return cloudprovider.ErrNotImplemented
}

// Id returns an unique identifier of the node group.
func (clng *CloudifyNodeGroup) Id() string {
	return clng.deploymentID + "." + clng.nodeID
}

// Debug returns a string containing all information regarding this node group.
func (clng *CloudifyNodeGroup) Debug() string {
	glog.Warningf("?Debug")
	return ""
}

// Nodes returns a list of all nodes that belong to this node group.
func (clng *CloudifyNodeGroup) Nodes() ([]string, error) {
	glog.Warningf("Nodes(%v.%v)", clng.deploymentID, clng.nodeID)

	node_instances_list := []string{}
	for _, cloud_node := range clng.getCloudifyNodes() {
		// filter nodes
		params := map[string]string{}
		params["deployment_id"] = clng.deploymentID
		params["node_id"] = cloud_node.Id
		cloud_instances := clng.client.GetNodeInstances(params)
		for _, instance := range cloud_instances.Items {
			// check runtime properties
			if instance.RuntimeProperties != nil {
				if v, ok := instance.RuntimeProperties["name"]; ok == true {
					switch v.(type) {
					case string:
						{
							node_instances_list = append(node_instances_list, v.(string))
						}
					}
				}
			}
		}
	}
	glog.Warningf("Nodes(%v.%v): %+v", clng.deploymentID, clng.nodeID, node_instances_list)
	return node_instances_list, nil
}

func (clng *CloudifyNodeGroup) getCurrentCharacteristics() (int64, int64) {
	var cpu int64 = 1
	var memory int64 = 512
	for _, cloud_node := range clng.getCloudifyNodes() {
		if cloud_node.Properties != nil {
			if v, ok := cloud_node.Properties["kubemem"]; ok == true {
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
			if v, ok := cloud_node.Properties["kubecpu"]; ok == true {
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
	glog.Warningf("getCurrentCharacteristics(%v.%v):cpu %v, mem: %v", clng.deploymentID, clng.nodeID, cpu, memory)
	return cpu, memory
}

// TemplateNodeInfo returns a schedulercache.NodeInfo structure of an empty
// (as if just started) node. This will be used in scale-up simulations to
// predict what would a new node look like if a node group was expanded. The returned
// NodeInfo is expected to have a fully populated Node object, with all of the labels,
// capacity and allocatable information as well as all pods that are started on
// the node by default, using manifest (most likely only kube-proxy). Implementation optional.
func (clng *CloudifyNodeGroup) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
	glog.Warningf("TemplateNodeInfo(%v.%v)", clng.deploymentID, clng.nodeID)

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

	glog.Warningf("Returned templateNodeInfo(%v.%v):%+v", clng.deploymentID, clng.nodeID, nodeInfo)
	return nodeInfo, nil
}

// Exist checks if the node group really exists on the cloud provider side. Allows to tell the
// theoretical node group from the real one. Implementation required.
func (clng *CloudifyNodeGroup) Exist() bool {
	glog.Warningf("Exist(%v.%v):#%v", clng.deploymentID, clng.nodeID, len(clng.getCloudifyNodes()))

	return len(clng.getCloudifyNodes()) > 0
}

// Create creates the node group on the cloud provider side. Implementation optional.
func (clng *CloudifyNodeGroup) Create() error {
	glog.Warningf("?Create")
	return cloudprovider.ErrNotImplemented
}

// Delete deletes the node group on the cloud provider side.
// This will be executed only for autoprovisioned node groups, once their size drops to 0.
// Implementation optional.
func (clng *CloudifyNodeGroup) Delete() error {
	glog.Warningf("?Delete")
	return cloudprovider.ErrNotImplemented
}

// Autoprovisioned returns true if the node group is autoprovisioned. An autoprovisioned group
// was created by CA and can be deleted when scaled to 0.
func (clng *CloudifyNodeGroup) Autoprovisioned() bool {
	glog.Warningf("?Autoprovisioned")
	return false
}

func CloudifyNodeToNodeGroup(client *cloudify.CloudifyClient, deployment, group_name string) *CloudifyNodeGroup {
	glog.Warningf("CloudifyNodeToNodeGroup(%v.%v)", deployment, group_name)
	return &CloudifyNodeGroup{
		client:       client,
		nodeID:       group_name,
		deploymentID: deployment,
	}
}
