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
	"github.com/cloudify-incubator/cloudify-rest-go-client/cloudify"
	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"fmt"
)

// CloudifyScaleProvider - settings for connect to cloudify
type CloudifyScaleProvider struct {
	client          *cloudify.Client
	deployment      string
	resourceLimiter *cloudprovider.ResourceLimiter
}

// GetDeploymentNodeInfo - return deployment info associated with cloudify
func (clsp *CloudifyScaleProvider) GetDeploymentNodeInfo() ([]map[string]string, error) {
	data, err := cloudify.ParseDeploymentFile(clsp.deployment)
	deploymentList := []map[string]string{}
	if err != nil {
		fmt.Errorf("Error While trying to parse deployment file")
		return nil, err
	}

	for _, deployment := range data.Deployments {
		deploymentInfo := make(map[string]string)
		dep := deployment.(map[string]interface{})

		deploymentInfo["id"] = dep["id"].(string)
		deploymentInfo["node_data_type"] = dep["node_data_type"].(string)
		deploymentInfo["deployment_type"] = dep["deployment_type"].(string)
		deploymentList = append(deploymentList, deploymentInfo)
	}

	return deploymentList, nil
}

// Name returns name of the cloud provider.
func (clsp *CloudifyScaleProvider) Name() string {
	glog.V(4).Infof("Name")
	return "cloudify"
}

// GetResourceLimiter returns struct containing limits (max, min) for resources (cores, memory etc.).
func (clsp *CloudifyScaleProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	glog.V(4).Infof("GetResourceLimiter")
	return clsp.resourceLimiter, nil
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state.
// In particular the list of node groups returned by NodeGroups can change as a result of CloudProvider.Refresh().
func (clsp *CloudifyScaleProvider) Refresh() error {
	glog.V(4).Infof("Refresh")
	// cleanup connection
	clsp.client.ResetConnection()
	// create cache for connection
	clsp.client.CacheConnection()
	return nil
}

// NodeGroups returns all node groups configured for this cloud provider.
func (clsp *CloudifyScaleProvider) NodeGroups() []cloudprovider.NodeGroup {
	nodes := []cloudprovider.NodeGroup{}
	items, err := clsp.GetDeploymentNodeInfo()
	if err != nil {
		glog.Errorf("Cloudify error: %s\n", err.Error())
	}

	if items == nil {
		glog.Errorf("no deployments found !!!")
	}

	for _, dep := range items {
		glog.V(4).Infof("NodeGroups, Deployment: %+v", dep["id"])
		deployment, err := clsp.client.GetDeployment(dep["id"])
		if err != nil {
			glog.Errorf("Cloudify error: %s\n", err.Error())
			return nodes
		}

		if deployment.ScalingGroups != nil {
			for groupName := range deployment.ScalingGroups {
				nodes = append(nodes, NodeToNodeGroup(clsp.client, dep["id"], groupName, dep["node_data_type"]))
			}
		}
	}
	glog.Warningf("NodeGroups: %+v", nodes)
	return nodes
}

// NodeGroupForNode returns the node group for the given node.
func (clsp *CloudifyScaleProvider) NodeGroupForNode(kubeNode *apiv1.Node) (cloudprovider.NodeGroup, error) {
	items, err := clsp.GetDeploymentNodeInfo()
	if err != nil {
		glog.Errorf("Cloudify error: %s\n", err.Error())
	}

	if items == nil {
		glog.Errorf("no deployments found !!!")
	}
	for _, dep := range items {
		if dep["deployment_type"] == "node" {
			glog.V(4).Infof("NodeGroupForNode(%v.%v)", dep["id"], kubeNode.Name)
			groupedInstances, err := clsp.client.GetDeploymentInstancesScaleGrouped(
				dep["id"], dep["node_data_type"])

			if err != nil {
				glog.Errorf("Cloudify error: %s\n", err.Error())
				return nil, err
			}

			for groupName, cloudInstances := range groupedInstances {
				// search instance
				for _, cloudInstance := range cloudInstances.Items {
					// check runtime properties
					if cloudInstance.GetStringProperty("hostname") != kubeNode.Name {
						continue
					}

					return NodeToNodeGroup(clsp.client, dep["id"], groupName, dep["node_data_type"]), nil
				}
			}
			glog.Warningf("NodeGroupForNode(%v.%v): Skiped, check list of members in scale group", dep["id"], kubeNode.Name)
		}
	}

	return nil, nil
}

// Pricing returns pricing model for this cloud provider or error if not available.
func (clsp *CloudifyScaleProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	glog.Errorf("?Pricing")
	return nil, cloudprovider.ErrNotImplemented
}

// GetAvailableMachineTypes get all machine types that can be requested from the cloud provider.
// Implementation optional.
func (clsp *CloudifyScaleProvider) GetAvailableMachineTypes() ([]string, error) {
	glog.Errorf("?GetAvailableMachineTypes")
	return []string{}, cloudprovider.ErrNotImplemented
}

// NewNodeGroup builds a theoretical node group based on the node definition provided. The node group is not automatically
// created on the cloud provider side. The node group is not returned by NodeGroups() until it is created.
func (clsp *CloudifyScaleProvider) NewNodeGroup(machineType string, labels map[string]string, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	glog.Errorf("?NewNodeGroup")
	return nil, cloudprovider.ErrNotImplemented
}

// Cleanup cleans up all resources before the cloud provider is removed
func (clsp *CloudifyScaleProvider) Cleanup() error {
	return nil
}
