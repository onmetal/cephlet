package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/util/workqueue"
)

const (
	namespace = "cephletvolume"

	subsystemControllerRuntime = "controller_runtime"
	subsystemWorkQueue         = "workqueue"

	LabelController = "controller"
	LabelName       = "name"
)

var (
	ControllerRuntimeReconcileErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: subsystemControllerRuntime,
		Name:      "reconcile_errors_total",
		Help:      "Total number of reconciliation errors per controller",
	}, []string{LabelController})

	ControllerRuntimeReconcileDuration = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Subsystem: subsystemControllerRuntime,
		Name:      "reconcile_duration_seconds",
		Help:      "Length of time per reconciliation per controller",
	}, []string{LabelController})

	ControllerRuntimeMaxConcurrentReconciles = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystemControllerRuntime,
		Name:      "max_concurrent_reconciles",
		Help:      "Maximum number of concurrent reconciles per controller",
	}, []string{LabelController})

	ControllerRuntimeActiveWorker = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystemControllerRuntime,
		Name:      "active_workers",
		Help:      "Number of currently used workers per controller",
	}, []string{LabelController})

	workqueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystemWorkQueue,
		Name:      "depth",
		Help:      "Current depth of workqueue",
	}, []string{LabelName})

	workqueueAdds = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: subsystemWorkQueue,
		Name:      "adds_total",
		Help:      "Total number of adds handled by workqueue",
	}, []string{LabelName})

	workqueueLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: subsystemWorkQueue,
		Name:      "queue_duration_seconds",
		Help:      "How long in seconds an item stays in workqueue before being requested",
		Buckets:   prometheus.ExponentialBuckets(10e-9, 10, 12),
	}, []string{LabelName})

	workqueueDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: subsystemWorkQueue,
		Name:      "work_duration_seconds",
		Help:      "How long in seconds processing an item from workqueue takes.",
		Buckets:   prometheus.ExponentialBuckets(10e-9, 10, 12),
	}, []string{LabelName})

	workqueueUnfinished = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystemWorkQueue,
		Name:      "unfinished_work_seconds",
		Help: "How many seconds of work has been done that " +
			"is in progress and hasn't been observed by work_duration. Large " +
			"values indicate stuck threads. One can deduce the number of stuck " +
			"threads by observing the rate at which this increases.",
	}, []string{LabelName})

	workqueueLongestRunningProcessor = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystemWorkQueue,
		Name:      "longest_running_processor_seconds",
		Help: "How many seconds has the longest running " +
			"processor for workqueue been running.",
	}, []string{LabelName})

	workqueueRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: subsystemWorkQueue,
		Name:      "retries_total",
		Help:      "Total number of retries handled by workqueue",
	}, []string{LabelName})
)

type WorkqueueMetricsProvider struct{}

func NewWorkqueueMetricsProvider() *WorkqueueMetricsProvider {
	return &WorkqueueMetricsProvider{}
}

func (w *WorkqueueMetricsProvider) NewDepthMetric(name string) workqueue.GaugeMetric {
	labels := prometheus.Labels{LabelName: name}
	return workqueueDepth.With(labels)

}

func (w *WorkqueueMetricsProvider) NewAddsMetric(name string) workqueue.CounterMetric {
	labels := prometheus.Labels{LabelName: name}
	return workqueueAdds.With(labels)
}

func (w *WorkqueueMetricsProvider) NewLatencyMetric(name string) workqueue.HistogramMetric {
	labels := prometheus.Labels{LabelName: name}
	return workqueueLatency.With(labels)

}

func (w *WorkqueueMetricsProvider) NewWorkDurationMetric(name string) workqueue.HistogramMetric {
	labels := prometheus.Labels{LabelName: name}
	return workqueueDuration.With(labels)
}

func (w *WorkqueueMetricsProvider) NewUnfinishedWorkSecondsMetric(name string) workqueue.SettableGaugeMetric {
	labels := prometheus.Labels{LabelName: name}
	return workqueueUnfinished.With(labels)

}

func (w *WorkqueueMetricsProvider) NewLongestRunningProcessorSecondsMetric(name string) workqueue.SettableGaugeMetric {
	labels := prometheus.Labels{LabelName: name}
	return workqueueLongestRunningProcessor.With(labels)
}

func (w *WorkqueueMetricsProvider) NewRetriesMetric(name string) workqueue.CounterMetric {
	labels := prometheus.Labels{LabelName: name}
	return workqueueRetries.With(labels)

}

func RegisterAllMetrics() error {
	collectors := []prometheus.Collector{
		ControllerRuntimeReconcileErrors,
		ControllerRuntimeReconcileDuration,
		ControllerRuntimeMaxConcurrentReconciles,
		ControllerRuntimeActiveWorker,

		workqueueDepth,
		workqueueAdds,
		workqueueLatency,
		workqueueDuration,
		workqueueUnfinished,
		workqueueLongestRunningProcessor,
		workqueueRetries,
	}

	for i := range collectors {
		err := prometheus.Register(collectors[i])
		if err != nil {
			return err
		}
	}

	return nil
}
