package theine

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Yiling-J/theine-go/internal"
	"github.com/Yiling-J/theine-go/internal/nvm"
)

type JsonSerializer[T any] struct{}

func (s *JsonSerializer[T]) Marshal(v T) ([]byte, error) {
	return json.Marshal(v)
}

func (s *JsonSerializer[T]) Unmarshal(raw []byte, v *T) error {
	return json.Unmarshal(raw, v)
}

type NvmBuilder[K comparable, V any] struct {
	file            string
	cacheSize       uint64
	blockSize       int
	bucketSize      int
	regionSize      int
	maxItemSize     int
	cleanRegionSize int
	bhPct           int
	bfSize          int
	errorHandler    func(err error)
	keySerializer   Serializer[K]
	valueSerializer Serializer[V]
}

func NewNvmBuilder[K comparable, V any](file string, cacheSize uint64) *NvmBuilder[K, V] {
	return &NvmBuilder[K, V]{
		file:            file,
		cacheSize:       cacheSize,
		blockSize:       4096,
		regionSize:      16 << 20, // 16mb
		cleanRegionSize: 3,
		bucketSize:      4 << 10, // 4kb
		bhPct:           10,      // 10%
		bfSize:          8,       // 8 bytes bloomfilter
		errorHandler:    func(err error) {},
	}
}

// Device block size in bytes (minimum IO granularity).
func (b *NvmBuilder[K, V]) BlockSize(size int) *NvmBuilder[K, V] {
	b.blockSize = size
	return b
}

// Block cache Region size in bytes.
func (b *NvmBuilder[K, V]) RegionSize(size int) *NvmBuilder[K, V] {
	b.regionSize = size
	return b
}

// Big hash bucket size in bytes.
func (b *NvmBuilder[K, V]) BucketSize(size int) *NvmBuilder[K, V] {
	b.bucketSize = size
	return b
}

// Percentage of space to reserve for BigHash. Set the percentage > 0 to enable BigHash.
// Set percentage to 100 to disable block cache.
func (b *NvmBuilder[K, V]) BigHashPct(pct int) *NvmBuilder[K, V] {
	b.bhPct = pct
	return b
}

// Maximum size of a small item to be stored in BigHash. Must be less than the bucket size.
func (b *NvmBuilder[K, V]) BigHashMaxItemSize(size int) *NvmBuilder[K, V] {
	b.maxItemSize = size
	return b
}

// Block cache clean region size.
func (b *NvmBuilder[K, V]) CleanRegionSize(size int) *NvmBuilder[K, V] {
	b.cleanRegionSize = size
	return b
}

// Nvm cache error handler.
func (b *NvmBuilder[K, V]) ErrorHandler(fn func(err error)) *NvmBuilder[K, V] {
	b.errorHandler = fn
	return b
}

// Nvm cache key serializer.
func (b *NvmBuilder[K, V]) KeySerializer(s Serializer[K]) *NvmBuilder[K, V] {
	b.keySerializer = s
	return b
}

// Nvm cache value serializer.
func (b *NvmBuilder[K, V]) ValueSerializer(s Serializer[V]) *NvmBuilder[K, V] {
	b.valueSerializer = s
	return b
}

func (b *NvmBuilder[K, V]) BucketBfSize(size int) *NvmBuilder[K, V] {
	b.bfSize = size
	return b
}

// Build cache.
func (b *NvmBuilder[K, V]) Build() (*nvm.NvmStore[K, V], error) {
	if b.keySerializer == nil {
		b.keySerializer = &JsonSerializer[K]{}
	}
	if b.valueSerializer == nil {
		b.valueSerializer = &JsonSerializer[V]{}
	}
	return nvm.NewNvmStore[K, V](
		b.file, b.blockSize, b.cacheSize, b.bucketSize,
		b.regionSize, b.cleanRegionSize, uint8(b.bhPct), b.maxItemSize, b.bfSize, b.errorHandler,
		b.keySerializer, b.valueSerializer,
	)
}

type LoadingNvmStore[K comparable, V any] struct {
	loader func(ctx context.Context, key K) (Loaded[V], error)
	*nvm.NvmStore[K, V]
	groups []*internal.Group[K, Loaded[V]]
	group  *internal.Group[K, Loaded[V]]
	// hasher *internal.Hasher[K]
}

func NewLoadingNvmStore[K comparable, V any](nvmStore *nvm.NvmStore[K, V]) *LoadingNvmStore[K, V] {
	s := &LoadingNvmStore[K, V]{
		NvmStore: nvmStore,
		groups:   make([]*internal.Group[K, Loaded[V]], 1024),
		group:    internal.NewGroup[K, Loaded[V]](),
		//		hasher:   internal.NewHasher(nil),
	}

	for i := 0; i < len(s.groups); i++ {
		s.groups[i] = internal.NewGroup[K, Loaded[V]]()
	}
	return s
}

func (s *LoadingNvmStore[K, V]) Loader(loader func(ctx context.Context, key K) (Loaded[V], error)) {
	s.loader = loader
}

//func (s *LoadingNvmStore[K, V]) index(key K) (uint64, int) {
//	base := s.hasher.hash(key)
//	h := ((base >> 16) ^ base) * 0x45d9f3b
//	h = ((h >> 16) ^ h) * 0x45d9f3b
//	h = (h >> 16) ^ h
//	return base, int(h & uint64(len(s.groups)-1))
//}

func (s *LoadingNvmStore[K, V]) Get(ctx context.Context, key K) (V, error) {
	vs, cost, expire, ok, err := s.NvmStore.Get(key)
	_ = cost
	_ = expire
	var notFound *internal.NotFound
	if err != nil && !errors.As(err, &notFound) {
		//		log.Printf("LoadingNvmStore err %v", err)
		return vs, err
	}

	if ok {
		return vs, nil
	}

	loaded, err, _ := s.group.Do(key, func() (Loaded[V], error) {
		loaded, err := s.loader(ctx, key)
		if err == nil {
			s.Set(key, loaded.Value, loaded.Cost, time.Now().Add(loaded.TTL).UnixNano())
		}
		//		log.Printf("LoadingNvmStore loaded err %v", err)
		return loaded, err
	})
	return loaded.Value, err
}

type NotFound struct{}

func (e *NotFound) Error() string {
	return "i: not found"
}
