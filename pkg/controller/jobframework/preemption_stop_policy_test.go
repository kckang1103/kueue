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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
)

func TestPreemptionStopPolicyFor(t *testing.T) {
	now := time.Now()
	evicted := func(reason string) metav1.Condition {
		return metav1.Condition{Type: kueue.WorkloadEvicted, Status: metav1.ConditionTrue, Reason: reason}
	}
	blockedSince := func(d time.Duration) metav1.Condition {
		return metav1.Condition{
			Type:               kueue.WorkloadDisruptionBlocked,
			Status:             metav1.ConditionTrue,
			Reason:             "EvictionRefused",
			LastTransitionTime: metav1.NewTime(now.Add(-d)),
		}
	}

	testCases := map[string]struct {
		gateEnabled bool
		conditions  []metav1.Condition
		want        PreemptionStopPolicy
	}{
		"gate disabled": {
			conditions: []metav1.Condition{evicted(kueue.WorkloadEvictedByPreemption)},
			want:       PreemptionStopPolicyDelete,
		},
		"evicted by preemption": {
			gateEnabled: true,
			conditions:  []metav1.Condition{evicted(kueue.WorkloadEvictedByPreemption)},
			want:        PreemptionStopPolicyEvict,
		},
		"evicted for another reason": {
			gateEnabled: true,
			conditions:  []metav1.Condition{evicted(kueue.WorkloadEvictedByPodsReadyTimeout)},
			want:        PreemptionStopPolicyDelete,
		},
		"not evicted": {
			gateEnabled: true,
			want:        PreemptionStopPolicyDelete,
		},
		"blocked for less than the timeout": {
			gateEnabled: true,
			conditions: []metav1.Condition{
				evicted(kueue.WorkloadEvictedByPreemption),
				blockedSince(DefaultPreemptionEvictionTimeout - time.Second),
			},
			want: PreemptionStopPolicyEvict,
		},
		"blocked for the timeout": {
			gateEnabled: true,
			conditions: []metav1.Condition{
				evicted(kueue.WorkloadEvictedByPreemption),
				blockedSince(DefaultPreemptionEvictionTimeout),
			},
			want: PreemptionStopPolicyDelete,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.PreemptionRespectsPodDisruptionBudgets, tc.gateEnabled)
			wl := &kueue.Workload{Status: kueue.WorkloadStatus{Conditions: tc.conditions}}
			if got := preemptionStopPolicyFor(wl, now, DefaultPreemptionEvictionTimeout); got != tc.want {
				t.Errorf("preemptionStopPolicyFor() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPreemptionStopPolicyFromContext(t *testing.T) {
	if got := PreemptionStopPolicyFrom(context.Background()); got != PreemptionStopPolicyDelete {
		t.Errorf("default policy = %v, want Delete", got)
	}
	ctx := WithPreemptionStopPolicy(context.Background(), PreemptionStopPolicyEvict)
	if got := PreemptionStopPolicyFrom(ctx); got != PreemptionStopPolicyEvict {
		t.Errorf("policy = %v, want Evict", got)
	}
}

func TestDisruptionBlockedBackoff(t *testing.T) {
	testCases := map[time.Duration]time.Duration{
		0:                1 * time.Second,
		1 * time.Second:  2 * time.Second,
		2 * time.Second:  2 * time.Second,
		3 * time.Second:  4 * time.Second,
		10 * time.Second: 8 * time.Second,
		20 * time.Second: 16 * time.Second,
		time.Minute:      30 * time.Second,
		time.Hour:        30 * time.Second,
	}
	for blockedFor, want := range testCases {
		if got := disruptionBlockedBackoff(blockedFor); got != want {
			t.Errorf("disruptionBlockedBackoff(%v) = %v, want %v", blockedFor, got, want)
		}
	}
}
