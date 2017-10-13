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
	"io"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

type CloudifyScaleProvider struct {
	client *cloudify.CloudifyClient
}

// Name returns name of the cloud provider.
func (cl *CloudifyScaleProvider) Name() string {
	return "cloudify"
}

// NodeGroups returns all node groups configured for this cloud provider.
func (cl *CloudifyScaleProvider) NodeGroups() []cloudprovider.NodeGroup {
	return []cloudprovider.NodeGroup{}
}

// NodeGroupForNode returns the node group for the given node.
func (cl *CloudifyScaleProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// Pricing returns pricing model for this cloud provider or error if not available.
func (cl *CloudifyScaleProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

// GetAvailableMachineTypes get all machine types that can be requested from the cloud provider.
// Implementation optional.
func (cl *CloudifyScaleProvider) GetAvailableMachineTypes() ([]string, error) {
	return []string{}, cloudprovider.ErrNotImplemented
}

// NewNodeGroup builds a theoretical node group based on the node definition provided. The node group is not automatically
// created on the cloud provider side. The node group is not returned by NodeGroups() until it is created.
func (cl *CloudifyScaleProvider) NewNodeGroup(machineType string, labels map[string]string, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func BuildCloudifyCloud(config io.Reader) (*CloudifyScaleProvider, error) {
	return nil, nil
}
