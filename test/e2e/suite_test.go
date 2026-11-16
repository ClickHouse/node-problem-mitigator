//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestCooldown matches the cooldown in the e2e Helm values.
const TestCooldown = 30 * time.Second

// StateNamespace contains the Helm release and controller state.
const StateNamespace = "node-problem-mitigator-e2e"

var (
	testCtx   context.Context
	cancelCtx context.CancelFunc
	k8sClient client.Client
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Helm-installed node-problem-mitigator with KWOK")
}

var _ = BeforeSuite(func() {
	testCtx, cancelCtx = context.WithCancel(context.Background())

	By("connecting to the kind cluster with the Helm-installed controller")
	cfg, err := loadRESTConfig()
	Expect(err).NotTo(HaveOccurred())
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if cancelCtx != nil {
		cancelCtx()
	}
})

func loadRESTConfig() (*rest.Config, error) {
	kubeconfig := os.Getenv("KUBECONFIG")
	ctxName := os.Getenv("KUBECONTEXT")
	if ctxName == "" {
		return nil, fmt.Errorf("KUBECONTEXT must be set for e2e tests (use 'make test-e2e' which sets it to a kind cluster)")
	}
	if !strings.HasPrefix(ctxName, "kind-") {
		return nil, fmt.Errorf("e2e tests require a kind context, got %q", ctxName)
	}
	if kubeconfig == "" {
		home, _ := os.UserHomeDir()
		kubeconfig = fmt.Sprintf("%s/.kube/config", home)
	}
	loader := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}
	overrides := &clientcmd.ConfigOverrides{}
	overrides.CurrentContext = ctxName
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
	if err != nil {
		return nil, err
	}

	// Double-check the API server is localhost — kind clusters always listen
	// on 127.0.0.1. This prevents running against a remote cluster even if
	// someone names their context "kind-something".
	if !strings.Contains(cfg.Host, "127.0.0.1") && !strings.Contains(cfg.Host, "localhost") {
		return nil, fmt.Errorf("API server %q is not localhost; refusing to run e2e tests against a remote cluster", cfg.Host)
	}

	return cfg, nil
}
