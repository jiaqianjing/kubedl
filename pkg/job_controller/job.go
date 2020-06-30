package job_controller

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pytorchv1 "github.com/alibaba/kubedl/api/pytorch/v1"
	"github.com/alibaba/kubedl/pkg/code_sync"
	apiv1 "github.com/alibaba/kubedl/pkg/job_controller/api/v1"
	commonutil "github.com/alibaba/kubedl/pkg/util"
	"github.com/alibaba/kubedl/pkg/util/k8sutil"
	log "github.com/sirupsen/logrus"
)

// Reasons for job events.
const (
	FailedDeleteJobReason     = "FailedDeleteJob"
	SuccessfulDeleteJobReason = "SuccessfulDeleteJob"
)

func (jc *JobController) deletePodsAndServices(runPolicy *apiv1.RunPolicy, job interface{}, pods []*v1.Pod) error {
	if len(pods) == 0 {
		return nil
	}

	// Delete nothing when the cleanPodPolicy is None.
	if *runPolicy.CleanPodPolicy == apiv1.CleanPodPolicyNone {
		return nil
	}

	for _, pod := range pods {
		if *runPolicy.CleanPodPolicy == apiv1.CleanPodPolicyRunning && pod.Status.Phase != v1.PodRunning {
			continue
		}
		if err := jc.Controller.DeletePod(job, pod); err != nil {
			return err
		}
		// Pod and service have the same name, thus the service could be deleted using pod's name.
		if err := jc.Controller.DeleteService(job, pod.Name, pod.Namespace); err != nil {
			return err
		}
	}
	return nil
}

// ReconcileJobs checks and updates replicas for each given ReplicaSpec.
// It will requeue the job in case of an error while creating/deleting pods/services.
// 检查并更新每个给定的 ReplicaSpec 的副本；
// 在 pods/services 创建/删除出现错误的时候会重新排队
func (jc *JobController) ReconcileJobs(
	job interface{},
	replicas map[apiv1.ReplicaType]*apiv1.ReplicaSpec,
	jobStatus apiv1.JobStatus,
	runPolicy *apiv1.RunPolicy) (result reconcile.Result, err error) {

	// 1. 转换各种 operator GVR 的数据类型, job --> metaObject  job --> runtimeObject
	metaObject, ok := job.(metav1.Object)
	jobName := metaObject.GetName()
	if !ok {
		return result, fmt.Errorf("job is not of type metav1.Object")
	}
	runtimeObject, ok := job.(runtime.Object)
	if !ok {
		return result, fmt.Errorf("job is not of type runtime.Object")
	}

	// 2. 根据 GVR 获取到对应唯一的 key: "namespace/name"
	jobKey, err := KeyFunc(job)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for job object %#v: %v", job, err))
		return result, err
	}
	log.Infof("Reconciling for job %s", metaObject.GetName())

	// 最后：看是否需要重新 reconcile, 如果需要将 key 扔到 BackoffStatesQueue, 否则清除
	defer func() {
		// Add job key into backoff-states queue since it will be retried in
		// next round util reconciling succeeded.
		if result.Requeue || err != nil {
			jc.BackoffStatesQueue.AddRateLimited(jobKey)
			return
		}
		// Job exits reconciling process and do not have to be retried, just
		// forget it.
		jc.BackoffStatesQueue.Forget(jobKey)
	}()

	// 3. 是否需要支持 Gang Scheduling, 如果支持，创建 Gang 实体：1. 创建 PodGroup 2. 关联 job
	if jc.Config.EnableGangScheduling {
		log.Infof("gang schedule enabled, start to syncing for job %s", jobKey)
		if _, err = jc.CreateGang(metaObject, replicas); err != nil {
			return result, err
		}
	}

	oldStatus := jobStatus.DeepCopy()

	// 4. 注入拉取 git 代码的 initContainer
	err = code_sync.InjectCodeSyncInitContainers(metaObject, replicas)
	if err != nil {
		log.Error(err, "failed to inject code sync init container")
		return reconcile.Result{}, err
	}
	// TODO(SimonCqk): update job conditions failed ?

	// 5. 获取 job 所有的 pod, label 带有 job-name: "${JOB_NAME}"
	pods, err := jc.Controller.GetPodsForJob(job)
	if err != nil {
		log.Warnf("GetPodsForJob error %v", err)
		return result, err
	}

	// 6. 获取 job 所有的 service
	services, err := jc.Controller.GetServicesForJob(job)
	if err != nil {
		log.Warnf("GetServicesForJob error %v", err)
		return result, err
	}

	// retrieve the previous number of retry
	// 获取之前 job 重试的次数
	previousRetry := jc.BackoffStatesQueue.NumRequeues(jobKey)

	// 7. 获取 job 状态处于非 Succeeded、Failed 的 Pods
	activePods := k8sutil.FilterActivePods(pods)
	// 当前 job pods 处于 active 的 数量
	active := int32(len(activePods))
	// 当前 job pods 处于 Failed 的数量
	failed := k8sutil.FilterPodCount(pods, v1.PodFailed)
	// job 期望设置 pod 的数量
	totalReplicas := k8sutil.GetTotalReplicas(replicas)
	// Master 和 Worker 中 pod 状态为 Failed 的总数量
	prevReplicasFailedNum := k8sutil.GetTotalFailedReplicas(jobStatus.ReplicaStatuses)

	var failureMessage string
	jobExceedsLimit := false
	// 检查当前失败的 pod 数是否超过期望的最大重试次数（BackoffLimit），默认 false, 即无限重试次数
	exceedsBackoffLimit := false
	// 检查 job 所有 container 的 restartCounts 的总和，否超过期望的最大重试次数（BackoffLimit），默认 false, 即无限重试次数
	pastBackoffLimit := false

	if runPolicy.BackoffLimit != nil {
		jobHasNewFailure := failed > prevReplicasFailedNum
		// new failures happen when status does not reflect the failures and active
		// is different than parallelism, otherwise the previous controller loop
		// failed updating status so even if we pick up failure it is not a new one
		exceedsBackoffLimit = jobHasNewFailure && (active != totalReplicas) &&
			(int32(previousRetry)+1 > *runPolicy.BackoffLimit)

		// container restartCounts 是否超过最大重启次数，默认不设置 runPolicy.BackoffLimit, 为无限重试次数
		pastBackoffLimit, err = jc.pastBackoffLimit(jobName, runPolicy, replicas, pods)
		if err != nil {
			return result, err
		}
	}

	// 判断当前 job 是否超过 BackoffLimit ， 超过了直接置为 Failed
	// 没有超过，再看 job 的 ActiveDeadlineSeconds 是否超过了(当前时刻 - jobStatus.StartTime.Time)，超过了直接置为 Failed
	if exceedsBackoffLimit || pastBackoffLimit {
		// check if the number of pod restart exceeds backoff (for restart OnFailure only)
		// OR if the number of failed jobs increased since the last syncJob
		jobExceedsLimit = true
		failureMessage = fmt.Sprintf("Job %s has failed because it has reached the specified backoff limit", jobName)
	} else if jc.pastActiveDeadline(runPolicy, jobStatus) {
		failureMessage = fmt.Sprintf("Job %s has failed because it was active longer than specified deadline", jobName)
		jobExceedsLimit = true
		now := metav1.Now()
		jobStatus.CompletionTime = &now
	}

	// If the Job is terminated, delete all pods and services.
	// Job Succeeded or Failed， 结合 CleanPodPolicy 删除需要的 pod 和 service， 结合 TTLSecondsAfterFinished 删除需要删除的 Job
	if commonutil.IsSucceeded(jobStatus) || commonutil.IsFailed(jobStatus) || jobExceedsLimit {
		if err = jc.deletePodsAndServices(runPolicy, job, pods); err != nil {
			return result, err
		}

		// 结合 TTLSecondsAfterFinished 删除需要删除的 Job
		if result, err = jc.cleanupJob(runPolicy, jobStatus, job); err != nil {
			return result, err
		}

		if jc.Config.EnableGangScheduling {
			// 删除 Gang 的 PodGroup
			jc.Recorder.Event(runtimeObject, v1.EventTypeNormal, "JobTerminated", "Job has been terminated. Deleting PodGroup")
			if err = jc.DeleteGang(metaObject); err != nil {
				jc.Recorder.Eventf(runtimeObject, v1.EventTypeWarning, "FailedDeletePodGroup", "Error deleting: %v", err)
				return result, err
			} else {
				jc.Recorder.Eventf(runtimeObject, v1.EventTypeNormal, "SuccessfulDeletePodGroup", "Deleted PodGroup: %v", jobName)
			}
		}

		if jobExceedsLimit {
			// 超过了 BackoffLimit 、ActiveDeadlineSeconds 将任务更新为 Failed
			jc.Recorder.Event(runtimeObject, v1.EventTypeNormal, commonutil.JobFailedReason, failureMessage)
			if jobStatus.CompletionTime == nil {
				now := metav1.Now()
				jobStatus.CompletionTime = &now
			}
			err = commonutil.UpdateJobConditions(&jobStatus, apiv1.JobFailed, commonutil.JobFailedReason, failureMessage)
			if err != nil {
				log.Infof("Append job condition error: %v", err)
				return result, err
			}
		}

		// At this point the pods may have been deleted.
		// 1) If the job succeeded, we manually set the replica status.
		// 2) If any replicas are still active, set their status to succeeded.
		if commonutil.IsSucceeded(jobStatus) {
			for rtype := range jobStatus.ReplicaStatuses {
				jobStatus.ReplicaStatuses[rtype].Succeeded += jobStatus.ReplicaStatuses[rtype].Active
				jobStatus.ReplicaStatuses[rtype].Active = 0
			}
		}
		if !reflect.DeepEqual(*oldStatus, jobStatus) {
			return result, jc.Controller.UpdateJobStatusInApiServer(job, &jobStatus)
		}
		return result, nil
	}

	// Save the current state of the replicas
	// 貌似没用到这个变量
	replicasStatus := make(map[string]v1.PodPhase)
	// pod 是否在重启，在 ReconcilePods() 会更新这个值，该值也影响到 job 状态的更新
	restart := false

	// Diff current active pods/services with replicas.
	// 遍历 Job 的 replica， GetReconcileOrders() 确定遍历 replica 不同类型的顺序，如：pytorch 先遍历 Master 再遍历 Worker
	for _, rtype := range jc.Controller.GetReconcileOrders() {
		spec, exist := replicas[rtype]
		if !exist {
			continue
		}
		// 按照 replica-type 同步下面的 Pod：
		// 1. 创建 pod
		// 2. 更新 job 的 ReplicaStatuses
		// 3. 更新 restart
		err = jc.ReconcilePods(metaObject, &jobStatus, pods, rtype, spec, replicasStatus, replicas, &restart)
		if err != nil {
			log.Warnf("ReconcilePods error %v", err)
			return result, err
		}

		// Service is in need only for Master
		if jc.Controller.GetAPIGroupVersionKind().Kind == pytorchv1.Kind &&
			rtype != pytorchv1.PyTorchReplicaTypeMaster {
			continue
		}

		// 同步 Service
		err = jc.ReconcileServices(metaObject, services, rtype, spec)
		if err != nil {
			log.Warnf("ReconcileServices error %v", err)
			return result, err
		}
	}

	err = jc.Controller.UpdateJobStatus(job, replicas, &jobStatus, restart)
	if err != nil {
		log.Warnf("UpdateJobStatus error %v", err)
		return result, err
	}

	// Metering first pod launch delay when job state transit from created to running.
	if commonutil.IsCreated(*oldStatus) && commonutil.IsRunning(jobStatus) {
		jc.Metrics.FirstPodLaunchDelaySeconds(activePods, metaObject, jobStatus)
	}

	// Metring all pods launch delay when latest pods are all active after reconciled, and previous
	// job status has not reached a all-active state, including the following cases:
	// 1. job created, successfully create all pods and becomes job running.
	// 2. job created, create some pods while some pods failed, finally becomes job running.
	// 3. job running then some pods failed, job step into restarting state, then pod recreated and
	//    finally return back to running state.
	//
	// case 3 should be discarded.
	if (k8sutil.GetTotalAvtiveReplicas(jobStatus.ReplicaStatuses) == totalReplicas) &&
		(k8sutil.GetTotalAvtiveReplicas(oldStatus.ReplicaStatuses) < totalReplicas) &&
		!commonutil.IsRestarting(*oldStatus) {
		jc.Metrics.AllPodsLaunchDelaySeconds(pods, metaObject, jobStatus)
	}

	// No need to update the job status if the status hasn't changed since last time.
	if !reflect.DeepEqual(*oldStatus, jobStatus) {
		return result, jc.Controller.UpdateJobStatusInApiServer(job, &jobStatus)
	}
	return result, nil
}

// pastActiveDeadline checks if job has ActiveDeadlineSeconds field set and if it is exceeded.
// pastActiveDeadline 检查作业是否设置了 ActiveDeadlineSeconds 字段，以及是否超过了该字段的值。
func (jc *JobController) pastActiveDeadline(runPolicy *apiv1.RunPolicy, jobStatus apiv1.JobStatus) bool {
	if runPolicy.ActiveDeadlineSeconds == nil || jobStatus.StartTime == nil {
		return false
	}
	now := metav1.Now()
	start := jobStatus.StartTime.Time
	duration := now.Time.Sub(start)
	allowedDuration := time.Duration(*runPolicy.ActiveDeadlineSeconds) * time.Second
	return duration >= allowedDuration
}

// pastBackoffLimit checks if container restartCounts sum exceeds BackoffLimit
// this method applies only to pods with restartPolicy == OnFailure or Always
// pastBackoffLimit 检查 Job 所有 container 的 restartCounts 总和是否超过 BackoffLimit
// 此方法仅适用于 restartPolicy==OnFailure 或 Always 的 pod
func (jc *JobController) pastBackoffLimit(jobName string, runPolicy *apiv1.RunPolicy,
	replicas map[apiv1.ReplicaType]*apiv1.ReplicaSpec, pods []*v1.Pod) (bool, error) {
	// 如果没设置 BackoffLimit, 永远没有超过最大限制，默认无限重试
	if runPolicy.BackoffLimit == nil {
		return false, nil
	}
	result := int32(0)
	for rtype, spec := range replicas {
		// 如果 replica 的 RestartPolicy 不是 "OnFailure" or "Always"，并且 BackoffLimit > 0, 永远没有超过最大限制
		if spec.RestartPolicy != apiv1.RestartPolicyOnFailure && spec.RestartPolicy != apiv1.RestartPolicyAlways {
			log.Warnf("The restart policy of replica %v of the job %v is not OnFailure or Always. Not counted in backoff limit.", rtype, jobName)
			continue
		}
		// Convert ReplicaType to lower string.
		rt := strings.ToLower(string(rtype))
		// 从 job 所有的 pods 取回对应 replica-type 的 pods
		pods, err := jc.FilterPodsForReplicaType(pods, rt)
		if err != nil {
			return false, err
		}
		for i := range pods {
			po := pods[i]
			if po.Status.Phase != v1.PodRunning {
				continue
			}
			for j := range po.Status.InitContainerStatuses {
				stat := po.Status.InitContainerStatuses[j]
				result += stat.RestartCount
			}
			for j := range po.Status.ContainerStatuses {
				stat := po.Status.ContainerStatuses[j]
				result += stat.RestartCount
			}
		}
	}

	if *runPolicy.BackoffLimit == 0 {
		return result > 0, nil
	}
	return result >= *runPolicy.BackoffLimit, nil
}

func (jc *JobController) cleanupJob(runPolicy *apiv1.RunPolicy, jobStatus apiv1.JobStatus, job interface{}) (reconcile.Result, error) {
	currentTime := time.Now()
	metaObject, _ := job.(metav1.Object)
	res := reconcile.Result{}
	ttl := runPolicy.TTLSecondsAfterFinished
	if ttl == nil {
		return res, nil
	}
	if jobStatus.CompletionTime == nil {
		return res, fmt.Errorf("cleanup Job %s, but job has CompletionTime not set", metaObject.GetName())
	}
	duration := time.Second * time.Duration(*ttl)
	deleteTime := jobStatus.CompletionTime.Add(duration)
	if currentTime.After(deleteTime) {
		err := jc.Controller.DeleteJob(job)
		if err != nil {
			commonutil.LoggerForJob(metaObject).Warnf("Cleanup Job error: %v.", err)
			return res, err
		}
		return res, nil
	}
	res.Requeue = true
	res.RequeueAfter = deleteTime.Sub(currentTime)
	return res, nil
}
