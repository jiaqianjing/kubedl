/*
Copyright 2019 The Alibaba Authors.

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

package controllers

import (
	"github.com/alibaba/kubedl/pkg/job_controller"
	"github.com/alibaba/kubedl/pkg/util/workloadgate"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"
	controllerruntime "sigs.k8s.io/controller-runtime"
)

// SetupWithManagerFunc is a list of functions to setup all controllers to the manager.
// 将所有自定义的 workload 的 controller 塞到 manager 的 map 里， key 是定义 GVK 的对象（如 pytorchjob）
// value 是对应 GVK 的 reconciler.SetupWithManager(). (在 import package controllers 时通过 add.*.go 文件中的 init 函数完成注册)
var SetupWithManagerMap = make(map[runtime.Object]func(mgr controllerruntime.Manager, config job_controller.JobControllerConfiguration) error)

// SetupWithManager setups all controllers to the manager.
// 完成所有 controller 的 reconciler 的注册
func SetupWithManager(mgr controllerruntime.Manager, config job_controller.JobControllerConfiguration) error {
	for workload, f := range SetupWithManagerMap {
		kind, enabled := workloadgate.IsWorkloadEnable(workload, mgr.GetScheme())
		if !enabled {
			klog.Warningf("skip workload %v because it is not enabled.", kind)
			continue
		}
		if err := f(mgr, config); err != nil {
			return err
		}
		klog.Infof("workload %v controller has started.", kind)
	}
	return nil
}
