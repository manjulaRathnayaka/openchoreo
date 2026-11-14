// Copyright 2025 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"math/rand/v2"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// States for conditions
const (
	TypeAccepted    = "Accepted"
	TypeProgressing = "Progressing"
	TypeAvailable   = "Available"
	TypeCreated     = "Created"
	TypeReady       = "Ready"
	TypeTerminating = "Terminating"
)

// Status update intervals
const (
	// StatusUpdateInterval is the interval at which controllers should refresh status fields
	// This is used for periodic status updates like agent connection status
	StatusUpdateInterval = 1 * time.Minute

	// statusUpdateJitter spreads periodic status refreshes. Plane CRs created
	// together (a bulk onboarding of tenants) would otherwise requeue in
	// lockstep and hit the controller and the cluster gateway in bursts.
	statusUpdateJitter = 15 * time.Second

	// PlaneStatusWorkers is the minimum number of concurrent reconciles for
	// plane controllers. Each plane CR polls the cluster gateway for agent
	// status every StatusUpdateInterval; with thousands of tenant CRs a single
	// worker cannot keep up, and a newly created CR then waits behind the
	// periodic refreshes before its agents are authorized for it.
	PlaneStatusWorkers = 4
)

// StatusRequeueAfter returns StatusUpdateInterval plus random jitter.
func StatusRequeueAfter() time.Duration {
	//nolint:gosec // Spreading load, not a security decision.
	return StatusUpdateInterval + time.Duration(rand.Int64N(int64(statusUpdateJitter)))
}

// PlaneSpecChangedPredicate passes plane CR events that need a reconcile and
// drops status-only updates. Plane controllers write agent connection status
// on every periodic refresh; without this filter each write triggers another
// reconcile, doubling the work for every plane CR.
//
// Marking an object for deletion (deletionTimestamp) or changing its
// finalizers does not change metadata.generation, so those updates are passed
// explicitly: finalization must run promptly, not on the next periodic
// refresh.
func PlaneSpecChangedPredicate() predicate.Predicate {
	return predicate.Or(
		predicate.GenerationChangedPredicate{},
		predicate.LabelChangedPredicate{},
		predicate.AnnotationChangedPredicate{},
		predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				if e.ObjectOld == nil || e.ObjectNew == nil {
					return true
				}
				if (e.ObjectOld.GetDeletionTimestamp() == nil) != (e.ObjectNew.GetDeletionTimestamp() == nil) {
					return true
				}
				return !slices.Equal(e.ObjectOld.GetFinalizers(), e.ObjectNew.GetFinalizers())
			},
			CreateFunc:  func(event.CreateEvent) bool { return false },
			DeleteFunc:  func(event.DeleteEvent) bool { return false },
			GenericFunc: func(event.GenericEvent) bool { return false },
		},
	)
}

// UpdateCondition updates or adds a condition to any resource that has a Status with Conditions
func UpdateCondition(
	ctx context.Context,
	c client.StatusWriter,
	resource client.Object,
	conditions *[]metav1.Condition,
	conditionType string,
	status metav1.ConditionStatus,
	reason, message string,
) error {
	logger := log.FromContext(ctx)

	// Built through NewCondition so the message bound applies here too - this helper is
	// the other way a condition reaches a status write.
	condition := NewCondition(ConditionType(conditionType), status, ConditionReason(reason),
		message, resource.GetGeneration())

	changed := meta.SetStatusCondition(conditions, condition)
	if changed {
		logger.Info("Updating Resource status",
			"Resource.Kind", resource.GetObjectKind().GroupVersionKind().Kind,
			"Resource.Name", resource.GetName())

		if err := c.Update(ctx, resource); err != nil {
			logger.Error(err, "Failed to update resource status",
				"Resource.Kind", resource.GetObjectKind().GroupVersionKind().Kind,
				"Resource.Name", resource.GetName())
			return err
		}

		logger.Info("Updated Resource status",
			"Resource.Kind", resource.GetObjectKind().GroupVersionKind().Kind,
			"Resource.Name", resource.GetName())
	}
	return nil
}
