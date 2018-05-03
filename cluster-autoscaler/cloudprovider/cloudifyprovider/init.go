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
	"io"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

// BuildCloudifyCloud - create cloudify provider instance
func BuildCloudifyCloud(config io.Reader, resourceLimiter *cloudprovider.ResourceLimiter) (*CloudifyScaleProvider, error) {
	glog.Warning("New Cloudify client")

	cloudConfig, err := cloudify.ServiceClientInit(config)
	if err != nil {
		return nil, err
	}

	return &CloudifyScaleProvider{
		client:          cloudify.NewClient(cloudConfig.ClientConfig),
		deployment:      cloudConfig.DeploymentsFile,
		resourceLimiter: resourceLimiter,
	}, nil
}
