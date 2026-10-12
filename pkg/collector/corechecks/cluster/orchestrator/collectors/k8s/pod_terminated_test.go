// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build kubeapiserver && test

package k8s

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/fx"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	model "github.com/DataDog/agent-payload/v5/process"
	"github.com/DataDog/datadog-agent/comp/core"
	taggerfxmock "github.com/DataDog/datadog-agent/comp/core/tagger/fx-mock"
	workloadmeta "github.com/DataDog/datadog-agent/comp/core/workloadmeta/def"
	workloadmetafxmock "github.com/DataDog/datadog-agent/comp/core/workloadmeta/fx-mock"
	workloadmetamock "github.com/DataDog/datadog-agent/comp/core/workloadmeta/mock"
	"github.com/DataDog/datadog-agent/pkg/collector/corechecks/cluster/orchestrator/collectors"
	mockconfig "github.com/DataDog/datadog-agent/pkg/config/mock"
	"github.com/DataDog/datadog-agent/pkg/orchestrator"
	orchestratorconfig "github.com/DataDog/datadog-agent/pkg/orchestrator/config"
	"github.com/DataDog/datadog-agent/pkg/util/fxutil"
	"github.com/DataDog/datadog-agent/pkg/util/kubernetes/apiserver"
)

func TestTerminatedPodCollector(t *testing.T) {
	creationTime := CreateTestTime()
	deletionTime := metav1.NewTime(time.Date(2021, time.April, 16, 15, 30, 0, 0, time.UTC))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"annotation": "my-annotation",
			},
			CreationTimestamp: creationTime,
			DeletionTimestamp: &deletionTime,
			Labels: map[string]string{
				"app": "my-app",
			},
			Name:            "test-pod",
			Namespace:       "default",
			ResourceVersion: "1234",
			UID:             types.UID("e42e5adc-0749-11e8-a2b8-000c29dea4f6"),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx:latest",
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: 80,
							Protocol:      corev1.ProtocolTCP,
						},
					},
				},
			},
			// Pod is assigned to a node
			NodeName: "test-node",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}

	// Create dependencies using fxutil.Test with proper modules
	mockCfg := mockconfig.New(t)
	mockStore := fxutil.Test[workloadmetamock.Mock](t, fx.Options(
		core.MockBundle(),
		workloadmetafxmock.MockModule(workloadmeta.NewParams()),
	))
	mockTagger := taggerfxmock.SetupFakeTagger(t)

	collector := NewTerminatedPodCollector(mockCfg, mockStore, mockTagger)

	// The processor skips resources already seen in this global cache.
	orchestrator.KubernetesResourceCache.Delete(string(pod.UID))
	t.Cleanup(func() { orchestrator.KubernetesResourceCache.Delete(string(pod.UID)) })

	fakeClient := fake.NewClientset()
	fakeWatcher := watch.NewFakeWithOptions(watch.FakeOptions{ChannelSize: 1})
	fakeClient.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		podList := &corev1.PodList{
			ListMeta: metav1.ListMeta{ResourceVersion: "1000"},
		}
		return true, podList, nil
	})
	fakeClient.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
		return true, fakeWatcher, nil
	})

	orchestratorCfg := orchestratorconfig.NewDefaultOrchestratorConfig(nil)
	orchestratorCfg.KubeClusterName = "test-cluster"

	handledResources := make(chan handledResource, 1)
	stopCh := make(chan struct{})
	runCfg := &collectors.CollectorRunConfig{
		K8sCollectorRunConfig: collectors.K8sCollectorRunConfig{
			APIClient: &apiserver.APIClient{Cl: fakeClient},
		},
		ClusterID:   "test-cluster",
		Config:      orchestratorCfg,
		MsgGroupRef: atomic.NewInt32(0),
		StopCh:      stopCh,
		TerminatedResourceHandler: func(k8sCollector collectors.K8sCollector, obj any) {
			handledResources <- handledResource{collector: k8sCollector, obj: obj}
		},
	}

	collector.Init(runCfg)
	t.Cleanup(func() {
		close(stopCh)
		collector.watcher.AwaitStop()
	})

	t.Run("Success - Deleted pod is sent to the terminated resource handler", func(t *testing.T) {
		fakeWatcher.Delete(pod)

		select {
		case got := <-handledResources:
			expected := handledResource{collector: collector, obj: pod}
			assert.Equal(t, expected, got)
		case <-time.After(5 * time.Second):
			t.Fatal("deleted pod was not sent to the terminated resource handler")
		}
	})
	t.Run("Success - Informer is not used", func(t *testing.T) {
		assert.Nil(t, collector.Informer())
	})
	t.Run("Success - Process terminated pods", func(t *testing.T) {
		processResult, err := collector.Process(runCfg, []*corev1.Pod{pod})
		require.NoError(t, err)
		assert.Equal(t, 1, processResult.ResourcesListed)
		assert.Equal(t, 1, processResult.ResourcesProcessed)
		require.Len(t, processResult.Result.MetadataMessages, 1)
		require.Len(t, processResult.Result.ManifestMessages, 1)
		assert.IsType(t, &model.CollectorPod{}, processResult.Result.MetadataMessages[0])
		assert.IsType(t, &model.CollectorManifest{}, processResult.Result.ManifestMessages[0])
	})
	t.Run("Success - Run does not process any resource", func(t *testing.T) {
		runResult, err := collector.Run(runCfg)
		require.NoError(t, err)
		assert.Equal(t, 0, runResult.ResourcesListed)
		assert.Equal(t, 0, runResult.ResourcesProcessed)
		assert.Empty(t, runResult.Result.MetadataMessages)
		assert.Empty(t, runResult.Result.ManifestMessages)
	})
}

type handledResource struct {
	collector collectors.K8sCollector
	obj       any
}
