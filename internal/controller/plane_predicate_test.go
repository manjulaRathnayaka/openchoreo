// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	openchoreov1alpha1 "github.com/openchoreo/openchoreo/api/v1alpha1"
)

func TestPlaneSpecChangedPredicate(t *testing.T) {
	base := func() *openchoreov1alpha1.DataPlane {
		return &openchoreov1alpha1.DataPlane{ObjectMeta: metav1.ObjectMeta{
			Name: "dp", Namespace: "org", Generation: 1,
			Finalizers: []string{"openchoreo.dev/dataplane-cleanup"},
		}}
	}
	now := metav1.Now()
	p := PlaneSpecChangedPredicate()

	tests := []struct {
		name   string
		mutate func(o *openchoreov1alpha1.DataPlane)
		want   bool
	}{
		{"status-only update is dropped", func(o *openchoreov1alpha1.DataPlane) {
			o.Status.AgentConnection = &openchoreov1alpha1.AgentConnectionStatus{Connected: true}
		}, false},
		{"spec change passes", func(o *openchoreov1alpha1.DataPlane) { o.Generation = 2 }, true},
		{"deletion passes", func(o *openchoreov1alpha1.DataPlane) { o.DeletionTimestamp = &now }, true},
		{"finalizer change passes", func(o *openchoreov1alpha1.DataPlane) { o.Finalizers = nil }, true},
		{"label change passes", func(o *openchoreov1alpha1.DataPlane) { o.Labels = map[string]string{"a": "b"} }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old, upd := base(), base()
			tt.mutate(upd)
			if got := p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: upd}); got != tt.want {
				t.Fatalf("Update() = %v, want %v", got, tt.want)
			}
		})
	}
	if !p.Create(event.CreateEvent{Object: base()}) {
		t.Fatal("create events must pass")
	}
	if !p.Delete(event.DeleteEvent{Object: base()}) {
		t.Fatal("delete events must pass")
	}
}
