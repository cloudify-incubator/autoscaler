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
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

// CloudifyScaleProvider - settings for connect to cloudify
type CloudifyScaleProvider struct {
	client          *cloudify.Client
	deploymentID    string
	resourceLimiter *cloudprovider.ResourceLimiter
}

// Name returns name of the cloud provider.
func (clsp *CloudifyScaleProvider) Name() string {
	glog.V(4).Infof("Name: %v", clsp.deploymentID)
	return "cloudify"
}

// GetResourceLimiter returns struct containing limits (max, min) for resources (cores, memory etc.).
func (clsp *CloudifyScaleProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	glog.V(4).Infof("GetResourceLimiter: %v", clsp.deploymentID)
	return clsp.resourceLimiter, nil
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state.
// In particular the list of node groups returned by NodeGroups can change as a result of CloudProvider.Refresh().
func (clsp *CloudifyScaleProvider) Refresh() error {
	glog.Errorf("?Refresh: %v", clsp.deploymentID)
	return nil
}

// NodeGroups returns all node groups configured for this cloud provider.
func (clsp *CloudifyScaleProvider) NodeGroups() []cloudprovider.NodeGroup {
	glog.V(4).Infof("NodeGroups, Deployment: %+v", clsp.deploymentID)
	nodes := []cloudprovider.NodeGroup{}

	deployment, err := clsp.client.GetDeployment(clsp.deploymentID)
	if err != nil {
		glog.Errorf("Cloudify error: %s\n", err.Error())
		return nodes
	}

	if deployment.ScalingGroups != nil {
		for groupName := range deployment.ScalingGroups {
			nodes = append(nodes, NodeToNodeGroup(clsp.client, clsp.deploymentID, groupName))
		}
	}

	glog.Warningf("NodeGroups: %+v", nodes)
	return nodes
}

// NodeGroupForNode returns the node group for the given node.
func (clsp *CloudifyScaleProvider) NodeGroupForNode(kubeNode *apiv1.Node) (cloudprovider.NodeGroup, error) {
	glog.V(4).Infof("NodeGroupForNode(%v.%v)", clsp.deploymentID, kubeNode.Name)

	groupedInstances, err := clsp.client.GetDeploymentInstancesScaleGrouped(
		clsp.deploymentID, "cloudify.nodes.ApplicationServer.kubernetes.Node")

	if err != nil {
		glog.Errorf("Cloudify error: %s\n", err.Error())
		return nil, err
	}

	for groupName, cloudInstances := range groupedInstances {
		// search instance
		for _, cloudInstance := range cloudInstances.Items {
			// check runtime properties
			if cloudInstance.RuntimeProperties != nil {
				if v, ok := cloudInstance.RuntimeProperties["hostname"]; ok == true {
					switch v.(type) {
					case string:
						{
							if v.(string) != kubeNode.Name {
								// node with different name
								continue
							}
						}
					}
				} else {
					glog.Warningf("No name for %+v\n", cloudInstance.HostID)
					// node without name
					continue
				}
			} else {
				glog.Warningf("No properties for %+v\n", cloudInstance.HostID)
				// no properties
				continue
			}

			return NodeToNodeGroup(clsp.client, clsp.deploymentID, groupName), nil
		}
	}
	glog.Warningf("NodeGroupForNode(%v.%v): Skiped, check list of members in scale group", clsp.deploymentID, kubeNode.Name)
	return nil, nil
}

// Pricing returns pricing model for this cloud provider or error if not available.
func (clsp *CloudifyScaleProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	glog.Errorf("?Pricing: %v", clsp.deploymentID)
	return nil, cloudprovider.ErrNotImplemented
}

// GetAvailableMachineTypes get all machine types that can be requested from the cloud provider.
// Implementation optional.
func (clsp *CloudifyScaleProvider) GetAvailableMachineTypes() ([]string, error) {
	glog.Errorf("?GetAvailableMachineTypes: %v", clsp.deploymentID)
	return []string{}, cloudprovider.ErrNotImplemented
}

// NewNodeGroup builds a theoretical node group based on the node definition provided. The node group is not automatically
// created on the cloud provider side. The node group is not returned by NodeGroups() until it is created.
func (clsp *CloudifyScaleProvider) NewNodeGroup(machineType string, labels map[string]string, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	glog.Errorf("?NewNodeGroup(%v): %+v %+v %+v", clsp.deploymentID, machineType, labels, extraResources)
	return nil, cloudprovider.ErrNotImplemented
}

// Cleanup cleans up all resources before the cloud provider is removed
func (clsp *CloudifyScaleProvider) Cleanup() error {
	return nil
}
