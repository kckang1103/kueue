/*
Copyright The Kubernetes Authors.

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

package jobframework

import (
	"context"
	"fmt"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
)

// StopReasonWorkloadEvictedByPreemption is the reason stopJob passes to a ComposableJob's Stop
// for a Workload evicted by preemption ("WorkloadEvicted" + "DueTo" + the Evicted reason).
const StopReasonWorkloadEvictedByPreemption = StopReason(string(StopReasonWorkloadEvicted) + "DueTo" + kueue.WorkloadEvictedByPreemption)

const (
	// DefaultPreemptionEvictionTimeout bounds how long a refused eviction is retried before the
	// Pods are deleted. KEP-5273 makes it configurable through
	// Configuration.Integrations.PodPreemption.EvictionTimeout.
	DefaultPreemptionEvictionTimeout = 10 * time.Minute

	disruptionBlockedMinBackoff = time.Second
	disruptionBlockedMaxBackoff = 30 * time.Second
)

// PreemptionStopPolicy says how a ComposableJob stops the Pods of a Workload evicted by preemption.
type PreemptionStopPolicy int

const (
	// PreemptionStopPolicyDelete deletes the Pods (the behavior before KEP-5273).
	PreemptionStopPolicyDelete PreemptionStopPolicy = iota
	// PreemptionStopPolicyEvict stops the Pods through the eviction subresource, so
	// PodDisruptionBudgets are honored.
	PreemptionStopPolicyEvict
)

type preemptionStopPolicyKey struct{}

// WithPreemptionStopPolicy returns a context that tells Stop how to stop Pods evicted by preemption.
func WithPreemptionStopPolicy(ctx context.Context, policy PreemptionStopPolicy) context.Context {
	return context.WithValue(ctx, preemptionStopPolicyKey{}, policy)
}

// PreemptionStopPolicyFrom returns the policy set by WithPreemptionStopPolicy, or Delete.
func PreemptionStopPolicyFrom(ctx context.Context) PreemptionStopPolicy {
	if policy, ok := ctx.Value(preemptionStopPolicyKey{}).(PreemptionStopPolicy); ok {
		return policy
	}
	return PreemptionStopPolicyDelete
}

// ErrDisruptionBlocked is returned by Stop when a PodDisruptionBudget refused to evict some of the
// Pods. Those Pods keep running, so the Workload stays active and keeps its quota.
type ErrDisruptionBlocked struct {
	Pods []client.ObjectKey
}

func (e *ErrDisruptionBlocked) Error() string {
	return fmt.Sprintf("eviction of %d pod(s) refused by a PodDisruptionBudget: %v", len(e.Pods), e.Pods)
}

// preemptionStopPolicyFor decides how to stop wl's Pods: evict while the feature is enabled, wl
// was evicted by preemption, and its evictions have not been refused for longer than timeout.
func preemptionStopPolicyFor(wl *kueue.Workload, now time.Time, timeout time.Duration) PreemptionStopPolicy {
	if !features.Enabled(features.PreemptionRespectsPodDisruptionBudgets) {
		return PreemptionStopPolicyDelete
	}
	evCond := apimeta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadEvicted)
	if evCond == nil || evCond.Status != metav1.ConditionTrue || evCond.Reason != kueue.WorkloadEvictedByPreemption {
		return PreemptionStopPolicyDelete
	}
	if blocked := apimeta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadDisruptionBlocked); blocked != nil &&
		blocked.Status == metav1.ConditionTrue && now.Sub(blocked.LastTransitionTime.Time) >= timeout {
		return PreemptionStopPolicyDelete
	}
	return PreemptionStopPolicyEvict
}

// disruptionBlockedBackoff returns how long to wait before the next eviction attempt, given how
// long the refusals have lasted: retries fall 1s, 3s, 7s, 15s, ... after the first refusal, each
// wait doubling up to disruptionBlockedMaxBackoff.
func disruptionBlockedBackoff(blockedFor time.Duration) time.Duration {
	backoff := disruptionBlockedMinBackoff
	for nextRetry := disruptionBlockedMinBackoff; nextRetry <= blockedFor && backoff < disruptionBlockedMaxBackoff; nextRetry += backoff {
		backoff *= 2
	}
	return min(backoff, disruptionBlockedMaxBackoff)
}
