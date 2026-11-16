package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap/zapcore"
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apilabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/clickhouse/node-problem-mitigator/internal/controller"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")

	// version is the release version set with -ldflags "-X main.version=...".
	version string
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var dryRun bool
	var namespace string
	var showVersion bool
	var debug bool
	logFormat := "console"
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election, so that only one controller is active.")
	flag.BoolVar(&dryRun, "dry-run", false,
		"If set, the controller logs what it would do without mutating any nodes.")
	flag.StringVar(&namespace, "namespace", "",
		"Namespace for the controller's config ConfigMap (required).")
	flag.BoolVar(&showVersion, "version", false, "Print the version and exit.")
	flag.BoolVar(&debug, "debug", false, "Log at debug level.")
	flag.Func("log-format", "Log format: console or json. Defaults to console.", func(value string) error {
		if value != "console" && value != "json" {
			return fmt.Errorf("must be console or json")
		}
		logFormat = value
		return nil
	})
	flag.Parse()

	buildVersion := version
	if buildVersion == "" {
		buildVersion = "unknown"
	}
	if showVersion {
		fmt.Println(buildVersion)
		return
	}

	level := zapcore.InfoLevel
	if debug {
		level = zapcore.DebugLevel
	}
	opts := zap.Options{
		// Development mode turns off log sampling, which drops repeated messages.
		Development:     true,
		Level:           level,
		StacktraceLevel: zapcore.ErrorLevel,
	}
	encoder := zap.ConsoleEncoder
	if logFormat == "json" {
		encoder = zap.JSONEncoder
	}
	rfc3339 := func(c *zapcore.EncoderConfig) { c.EncodeTime = zapcore.RFC3339TimeEncoder }
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts), encoder(rfc3339)))

	controller.SetBuildInfo(buildVersion)

	if namespace == "" {
		setupLog.Error(fmt.Errorf("--namespace is required"), "Set --namespace to the controller's namespace")
		os.Exit(1)
	}

	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: false,
	}

	// Leader-election timings, relaxed from the controller-runtime defaults
	leaseDuration := 60 * time.Second
	renewDeadline := 40 * time.Second
	retryPeriod := 5 * time.Second

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsServerOptions,
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        "leader.npm.clickhouse.com",
		LeaderElectionNamespace: namespace,
		LeaseDuration:           &leaseDuration,
		RenewDeadline:           &renewDeadline,
		RetryPeriod:             &retryPeriod,
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.ConfigMap{}: {
					Label: apilabels.SelectorFromSet(
						apilabels.Set{
							"app.kubernetes.io/part-of": "node-problem-mitigator",
						},
					),
					Namespaces: map[string]cache.Config{
						namespace: {},
					},
				},
				// The Role grants Job access only in the controller's namespace.
				&batchv1.Job{}: {
					Label: apilabels.SelectorFromSet(
						apilabels.Set{
							"app.kubernetes.io/managed-by": "node-problem-mitigator",
						},
					),
					Namespaces: map[string]cache.Config{
						namespace: {},
					},
				},
			},
		},
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	if dryRun {
		setupLog.Info("Dry-run mode enabled: no nodes will be mutated")
	}
	setupLog.Info("Config will be read from ConfigMap",
		"configmap", controller.ControllerConfigMap)

	if err := (&controller.NodeReconciler{
		Client:     mgr.GetClient(),
		Timestamps: controller.NewTimestampStore(mgr.GetClient(), namespace),
		DryRun:     dryRun,
		Namespace:  namespace,
		APIReader:  mgr.GetAPIReader(),
		Recorder:   mgr.GetEventRecorder("node-problem-mitigator"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "Node")
		os.Exit(1)
	}

	// Dry run creates no Jobs, so there is nothing to report or delete.
	if !dryRun {
		if err := (&controller.JobReconciler{
			Client:    mgr.GetClient(),
			Namespace: namespace,
			Recorder:  mgr.GetEventRecorder("node-problem-mitigator"),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create controller", "controller", "Job")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
