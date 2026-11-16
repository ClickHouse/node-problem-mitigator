package controller

import (
	"context"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	StateConfigMap    = "node-problem-mitigator-state"
	lastMitigationKey = "last-mitigation-time"
	timestampFormat   = time.RFC3339
)

// TimestampStore tracks the last global mitigation operation in a ConfigMap.
// At most one node may be mitigated per cooldown window to prevent fleet-wide impact.
type TimestampStore struct {
	client    client.Client
	namespace string
	now       func() time.Time // for testing

	mu     sync.Mutex
	last   time.Time
	loaded bool
}

func NewTimestampStore(c client.Client, namespace string) *TimestampStore {
	return &TimestampStore{
		client:    c,
		namespace: namespace,
		now:       time.Now,
	}
}

// CanOperate returns true if the global cooldown has elapsed.
func (s *TimestampStore) CanOperate(ctx context.Context, cooldown time.Duration) bool {
	s.ensureLoaded(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.last.IsZero() {
		return true
	}
	return s.now().Sub(s.last) >= cooldown
}

// NextAllowed returns the duration until the next mitigation is allowed.
func (s *TimestampStore) NextAllowed(ctx context.Context, cooldown time.Duration) time.Duration {
	s.ensureLoaded(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.last.IsZero() {
		return 0
	}
	remaining := cooldown - s.now().Sub(s.last)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Record stores the current time as the last mitigation operation and persists it.
func (s *TimestampStore) Record(ctx context.Context) error {
	s.ensureLoaded(ctx)

	s.mu.Lock()
	prev := s.last
	s.last = s.now()
	s.mu.Unlock()

	if err := s.persist(ctx); err != nil {
		s.mu.Lock()
		s.last = prev
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *TimestampStore) ensureLoaded(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.loaded {
		return
	}

	log := logf.FromContext(ctx)

	var cm corev1.ConfigMap
	err := s.client.Get(ctx, types.NamespacedName{
		Name:      StateConfigMap,
		Namespace: s.namespace,
	}, &cm)

	if apierrors.IsNotFound(err) {
		s.loaded = true
		return
	}
	if err != nil {
		log.Error(err, "Failed to load timestamp ConfigMap, will retry on next reconcile")
		return
	}

	if ts, ok := cm.Data[lastMitigationKey]; ok {
		t, parseErr := time.Parse(timestampFormat, ts)
		if parseErr != nil {
			log.Error(parseErr, "Ignoring malformed timestamp", "value", ts)
		} else {
			s.last = t
		}
	}
	s.loaded = true
}

func (s *TimestampStore) persist(ctx context.Context) error {
	s.mu.Lock()
	val := s.last.Format(timestampFormat)
	s.mu.Unlock()

	data := map[string]string{lastMitigationKey: val}
	key := types.NamespacedName{Name: StateConfigMap, Namespace: s.namespace}

	var cm corev1.ConfigMap
	err := s.client.Get(ctx, key, &cm)
	if apierrors.IsNotFound(err) {
		cm = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      StateConfigMap,
				Namespace: s.namespace,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": controllerName,
					"app.kubernetes.io/part-of":    controllerName,
				},
			},
			Data: data,
		}
		return s.client.Create(ctx, &cm)
	}
	if err != nil {
		return err
	}

	cm.Data = data
	return s.client.Update(ctx, &cm)
}
