package plugins

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type CustomSchedulerArgs struct {
	Mode string `json:"mode"`
}

type CustomScheduler struct {
	handle    framework.Handle
	scoreMode string
}

var _ framework.PreFilterPlugin = &CustomScheduler{}
var _ framework.ScorePlugin = &CustomScheduler{}

// Name is the name of the plugin used in Registry and configurations.
const (
	Name              string = "CustomScheduler"
	groupNameLabel    string = "podGroup"
	minAvailableLabel string = "minAvailable"
	leastMode         string = "Least"
	mostMode          string = "Most"
)

func (cs *CustomScheduler) Name() string {
	return Name
}

// New initializes and returns a new CustomScheduler plugin.
func New(obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	cs := CustomScheduler{}
	mode := leastMode
	if obj != nil {
		args := obj.(*runtime.Unknown)
		var csArgs CustomSchedulerArgs
		if err := json.Unmarshal(args.Raw, &csArgs); err != nil {
			fmt.Printf("Error unmarshal: %v\n", err)
		}
		mode = csArgs.Mode
		if mode != leastMode && mode != mostMode {
			return nil, fmt.Errorf("invalid mode, got %s", mode)
		}
	}
	cs.handle = h
	cs.scoreMode = mode
	log.Printf("Custom scheduler runs with the mode: %s.", mode)

	return &cs, nil
}

// filter the pod if the pod in group is less than minAvailable
func (cs *CustomScheduler) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	log.Printf("Pod %s is in Prefilter phase.", pod.Name)
	newStatus := framework.NewStatus(framework.Success, "")

	// TODO
	// 1. extract the label of the pod
	podGroup := pod.GetLabels()[groupNameLabel]
	minAvailable, err := strconv.Atoi(pod.GetLabels()[minAvailableLabel])
	if err != nil {
		return nil, framework.NewStatus(framework.Error, fmt.Sprintf("invalid minAvailable value: %v", err))
	}
	// 2. retrieve the pod with the same group label
	sameLabelPods, err := cs.handle.SharedInformerFactory().Core().V1().Pods().Lister().List(labels.SelectorFromSet(labels.Set{groupNameLabel: podGroup}))
	if err != nil {
		return nil, framework.NewStatus(framework.Error, fmt.Sprintf("failed to list pods: %v", err))
	}
	// 3. justify if the pod can be scheduled
	if len(sameLabelPods) < minAvailable {
		return nil, framework.NewStatus(framework.Unschedulable, "not enough pods in the group")
	}

	return nil, newStatus
}

// PreFilterExtensions returns a PreFilterExtensions interface if the plugin implements one.
func (cs *CustomScheduler) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// Score invoked at the score extension point.
func (cs *CustomScheduler) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	log.Printf("Pod %s is in Score phase. Calculate the score of Node %s.", pod.Name, nodeName)

	// TODO
	// 1. retrieve the node allocatable memory
	nodeInfo, err := cs.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("failed to get node info: %v", err))
	}
	allocatableMemory := nodeInfo.Allocatable.Memory
	// 2. return the score based on the scheduler mode
	if cs.scoreMode == leastMode {
		return -allocatableMemory, framework.NewStatus(framework.Success)
	}

	return allocatableMemory, framework.NewStatus(framework.Success)
}

// ensure the scores are within the valid range
func (cs *CustomScheduler) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// TODO
	// find the range of the current score and map to the valid range
	var minScore, maxScore int64 = math.MaxInt64, math.MinInt64

	for _, score := range scores {
		if score.Score < minScore {
			minScore = score.Score
		}
		if score.Score > maxScore {
			maxScore = score.Score
		}
	}

	scoreRange := maxScore - minScore
	if scoreRange > 0 {
		for i := range scores {
			scores[i].Score = ((scores[i].Score - minScore) * 100) / scoreRange
		}
	} else {
		for i := range scores {
			scores[i].Score = 0
		}
	}

	return framework.NewStatus(framework.Success)
}

// ScoreExtensions of the Score plugin.
func (cs *CustomScheduler) ScoreExtensions() framework.ScoreExtensions {
	return cs
}
