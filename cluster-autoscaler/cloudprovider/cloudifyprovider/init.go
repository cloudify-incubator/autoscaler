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
	"encoding/json"
	"fmt"
	cloudify "github.com/cloudify-incubator/cloudify-rest-go-client/cloudify"
	"github.com/golang/glog"
	"io"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"os"
)

type CloudifyScaleProvider struct {
	client *cloudify.CloudifyClient
}

type CloudifyProviderConfig struct {
	Host       string `json:"host,omitempty"`
	User       string `json:"user,omitempty"`
	Password   string `json:"password,omitempty"`
	Tenant     string `json:"tenant,omitempty"`
	Deployment string `json:"deployment,omitempty"`
}

// Name returns name of the cloud provider.
func (cl *CloudifyScaleProvider) Name() string {
	glog.Warning("Name")
	return "cloudify"
}

// NodeGroups returns all node groups configured for this cloud provider.
func (cl *CloudifyScaleProvider) NodeGroups() []cloudprovider.NodeGroup {
	glog.Warning("NodeGroups")
	return []cloudprovider.NodeGroup{}
}

// NodeGroupForNode returns the node group for the given node.
func (cl *CloudifyScaleProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	glog.Warningf("NodeGroupForNode %+v", node)
	return nil, cloudprovider.ErrNotImplemented
}

// Pricing returns pricing model for this cloud provider or error if not available.
func (cl *CloudifyScaleProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	glog.Warning("Pricing")
	return nil, cloudprovider.ErrNotImplemented
}

// GetAvailableMachineTypes get all machine types that can be requested from the cloud provider.
// Implementation optional.
func (cl *CloudifyScaleProvider) GetAvailableMachineTypes() ([]string, error) {
	glog.Warning("GetAvailableMachineTypes")
	return []string{}, cloudprovider.ErrNotImplemented
}

// NewNodeGroup builds a theoretical node group based on the node definition provided. The node group is not automatically
// created on the cloud provider side. The node group is not returned by NodeGroups() until it is created.
func (cl *CloudifyScaleProvider) NewNodeGroup(machineType string, labels map[string]string, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	glog.Warningf("NewNodeGroup: %+v %+v %+v", machineType, labels, extraResources)
	return nil, cloudprovider.ErrNotImplemented
}

func BuildCloudifyCloud(config io.Reader) (*CloudifyScaleProvider, error) {
	glog.Warning("New Cloudify client")

	var cloudConfig CloudifyProviderConfig
	cloudConfig.Host = os.Getenv("CFY_HOST")
	cloudConfig.User = os.Getenv("CFY_USER")
	cloudConfig.Password = os.Getenv("CFY_PASSWORD")
	cloudConfig.Tenant = os.Getenv("CFY_TENANT")
	if config != nil {
		err := json.NewDecoder(config).Decode(&cloudConfig)
		if err != nil {
			return nil, err
		}
	}

	if len(cloudConfig.Host) == 0 {
		return nil, fmt.Errorf("You have empty host")
	}

	if len(cloudConfig.User) == 0 {
		return nil, fmt.Errorf("You have empty user")
	}

	if len(cloudConfig.Password) == 0 {
		return nil, fmt.Errorf("You have empty password")
	}

	if len(cloudConfig.Tenant) == 0 {
		return nil, fmt.Errorf("You have empty tenant")
	}

	if len(cloudConfig.Deployment) == 0 {
		return nil, fmt.Errorf("You have empty deployment")
	}

	glog.Warningf("Config %+v", cloudConfig)
	return &CloudifyScaleProvider{
		client: cloudify.NewClient(
			cloudConfig.Host, cloudConfig.User,
			cloudConfig.Password, cloudConfig.Tenant),
	}, nil
}
