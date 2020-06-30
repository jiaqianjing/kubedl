package job_controller

import (
	apiv1 "github.com/alibaba/kubedl/pkg/job_controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SatisfiedExpectations returns true if the required adds/dels for the given job have been observed.
// Add/del counts are established by the controller at sync time, and updated as controllees are observed by
// the controller manager.
// 被观测的 job 需要 adds/dels pod 满足要求 ，则 SatisfiedExpectations 返回 true
// 同时 Add/del 的计数器被创建，并在 controller manager 观察到受控对象时更新
// expectations 机制 --> https://zhuanlan.zhihu.com/p/110979963
func (jc *JobController) SatisfyExpectations(job metav1.Object, specs map[apiv1.ReplicaType]*apiv1.ReplicaSpec) bool {
	satisfied := true
	// 为 job 对象生成唯一的 key:  <namespace>/<object-name>
	key, err := KeyFunc(job)
	if err != nil {
		return false
	}
	for rtype := range specs {
		// Check the expectations of the pods.
		expectationPodsKey := GenExpectationPodsKey(key, string(rtype))
		// 满足条件 expectations:
		// 1. 该 key 在 ControllerExpectations 中没有查到，即首次创建
		// 2. 该 key 在 ControllerExpectations 中的 adds 和 dels 都 <= 0
		// 3. 该 key 在 ControllerExpectations 中已经超过5min没有更新了
		// 4. 调用 GetExpectations() 接口失败(该接口主要返回 ControlleeExpectations: 用来维护某个 key 的 add 和 del 这两个变量)
		satisfied = satisfied && jc.Expectations.SatisfiedExpectations(expectationPodsKey)

		// Check the expectations of the services.
		expectationServicesKey := GenExpectationServicesKey(key, string(rtype))
		satisfied = satisfied && jc.Expectations.SatisfiedExpectations(expectationServicesKey)
	}
	return satisfied
}
