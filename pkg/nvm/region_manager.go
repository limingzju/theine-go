package nvm

import (
	"bytes"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"

	"github.com/Yiling-J/theine-go/pkg"
	"github.com/Yiling-J/theine-go/pkg/alloc"
	"github.com/Yiling-J/theine-go/pkg/nvm/directio"
)

type Ordered interface {
	int | int8 | int16 | int32 | int64 | float32 | float64 | uint | uint8 | uint16 | uint32 | uint64
}

func Max[T Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

type Region struct {
	EndOffset uint64
	buffer    *bytes.Buffer
	clean     bool
	lock      *sync.RWMutex
}

type RegionManager struct {
	file             *os.File
	offset           uint64
	mu               sync.RWMutex
	sketchMu         sync.RWMutex
	active           uint32
	regionSize       uint32
	regionCount      uint32
	cleanRegionSize  uint32
	cleanRegionCount atomic.Int32
	cleanRegionDone  atomic.Bool
	regions          map[uint32]*Region
	sketch           *pkg.CountMinSketch
	bufferPool       sync.Pool
	removeRegion     func(data []byte, endOffset uint64) error
	readChan         chan uint32
	allocator        *alloc.Allocator
	flushChan        chan uint32
	cleanChan        chan uint32
	errorHandler     func(err error)
}

func NewRegionManager(offset uint64, regionSize, regionCount, cleanRegionSize uint32, removeFunc func(data []byte, endOffset uint64) error, allocator *alloc.Allocator, errHandler func(err error)) *RegionManager {
	rm := &RegionManager{
		offset:          offset,
		regionSize:      regionSize,
		regionCount:     regionCount,
		cleanRegionSize: cleanRegionSize,
		bufferPool: sync.Pool{New: func() any {
			return bytes.NewBuffer(directio.AlignedBlock(int(regionSize)))
		}},
		readChan:     make(chan uint32, 128),
		removeRegion: removeFunc,
		regions:      make(map[uint32]*Region, regionCount),
		sketch:       pkg.NewCountMinSketch(),
		allocator:    allocator,
		flushChan:    make(chan uint32, Max(uint32(3), cleanRegionSize)),
		cleanChan:    make(chan uint32, Max(uint32(3), cleanRegionSize)),
		errorHandler: errHandler,
	}
	for i := 0; i < int(regionCount); i++ {
		// region 0 is the first active region, so clean should be false
		rm.regions[uint32(i)] = &Region{EndOffset: 0, clean: i != 0, lock: &sync.RWMutex{}}
	}
	rm.cleanRegionCount.Store(int32(regionCount))
	rm.attachBuffer(rm.regions[0])
	rm.sketch.EnsureCapacity(uint(regionSize))
	go rm.readQ()
	for i := 0; i < int(cleanRegionSize); i++ {
		go rm.flushAndClean(i)
	}
	return rm
}

func (m *RegionManager) GetData(index *BlockInfo, rid uint64, offset uint64, size uint64) (*alloc.AllocItem, error) {
	region := m.regions[uint32(rid)]
	region.lock.RLock()
	if index.removed {
		region.lock.RUnlock()
		return nil, nil
	}
	item := m.allocator.Allocate(int(size))
	if region.buffer != nil {
		_ = copy(item.Data, region.buffer.Bytes()[offset:offset+size])
	} else {
		_, err := m.file.ReadAt(item.Data, int64(m.offset+rid*uint64(m.regionSize)+offset))
		if err != nil {
			region.lock.RUnlock()
			return item, err
		}
	}
	region.lock.RUnlock()
	m.readChan <- uint32(rid)
	return item, nil
}

func (m *RegionManager) Allocate(size int) (uint32, uint64, *bytes.Buffer, func(), error) {
	m.mu.Lock()
	region := m.regions[m.active]
	allocatedRegion := m.active
	if m.regionSize-uint32(region.EndOffset) < uint32(size) {
		full := m.active
		m.flushChan <- full
		clean := <-m.cleanChan
		m.active = clean
		allocatedRegion = clean
		region = m.regions[m.active]
		// reset offset and remove clean mark
		region.EndOffset = 0
		region.clean = false
		m.attachBuffer(region)
	}
	offset := region.EndOffset
	b := region.buffer.Bytes()[region.EndOffset : region.EndOffset+uint64(size)]
	region.EndOffset = region.EndOffset + uint64(size)
	buffer := bytes.NewBuffer(b)
	buffer.Reset()
	region.lock.Lock()
	m.mu.Unlock()
	callback := func() {
		region.lock.Unlock()
	}
	return allocatedRegion, offset, buffer, callback, nil
}

// reclaim should have rlock from caller
func (m *RegionManager) reclaim(threadId int) (uint32, error) {
	victim, clean := m.victim(threadId)
	if clean {
		return victim, nil
	} else {
		buffer := m.bufferPool.Get().(*bytes.Buffer)
		buffer.Reset()
		data := buffer.Bytes()[:m.regionSize]
		_, err := m.file.ReadAt(data, int64(m.offset+uint64(victim)*uint64(m.regionSize)))
		if err != nil {
			return victim, err
		}
		region := m.regions[victim]
		region.lock.Lock()
		err = m.removeRegion(data, region.EndOffset)
		m.bufferPool.Put(buffer)
		region.clean = true
		region.lock.Unlock()
		return victim, err
	}
}

func (m *RegionManager) flushSync(threadId int, rid uint32) error {
	region := m.regions[rid]
	region.lock.Lock()
	defer region.lock.Unlock()
	b := region.buffer.Bytes()[:m.regionSize]
	log.Printf("flushAndSync threadId=%d rid=%d size=%d", threadId, rid, m.regionSize)
	_, err := m.file.WriteAt(b, int64(m.offset+uint64(rid)*uint64(m.regionSize)))
	if err != nil {
		return err
	}
	m.detachBuffer(region)
	return nil
}

// flush and clean always come together
// because flush means current buffer is full and need a new clean buffer
func (m *RegionManager) flushAndClean(threadId int) {
	for rid := range m.flushChan {
		err := m.flushSync(threadId, rid)
		if err != nil {
			m.errorHandler(err)
			continue
		}
		var clean uint32
		if !m.cleanRegionDone.Load() {
			new := m.cleanRegionCount.Add(-1)
			if new >= int32(m.cleanRegionSize) {
				clean = uint32(new)
				log.Printf("!cleanRegionDone threadId %d cleanChan %d", threadId, clean)
				m.cleanChan <- clean
				continue
			} else {
				m.cleanRegionDone.Store(true)
			}
		}
		clean, err = m.reclaim(threadId)
		if err != nil {
			m.errorHandler(err)
			continue
		}
		log.Printf("threadId %d cleanChan %d", threadId, clean)
		m.cleanChan <- clean
	}
}

func (m *RegionManager) attachBuffer(region *Region) {
	region.buffer = m.bufferPool.Get().(*bytes.Buffer)
	region.buffer.Reset()
}
func (m *RegionManager) detachBuffer(region *Region) {
	buffer := region.buffer
	region.buffer = nil
	m.bufferPool.Put(buffer)
}

func (m *RegionManager) victim(threadId int) (uint32, bool) {
	clean := false
	log.Printf("victim %d start", threadId)
	counter := 0
	var new uint32
	var fq uint
	m.sketchMu.RLock()
	for {
		rid := uint32(rand.Intn(int(m.regionCount)))
		// skip if already clean or buffer not nil
		rg := m.regions[rid]
		rg.lock.RLock()
		log.Printf("victim %d regionCount=%d rid=%d clean=%v buffer_is_nil=%v", threadId, m.regionCount, rid, rg.clean, rg.buffer == nil)
		if rg.clean && rg.buffer == nil {
			rg.lock.RUnlock()
			return rid, true
		}
		if rg.clean || rg.buffer != nil {
			rg.lock.RUnlock()
			continue
		}
		rg.lock.RUnlock()
		fqn := m.sketch.Estimate(spread(uint64(rid)))
		if new == 0 || fqn < fq {
			fq = fqn
			new = rid
		}
		counter += 1
		if counter == 5 {
			break
		}
	}
	m.sketchMu.RUnlock()
	log.Printf("victim %d return rid=%d", threadId, new)
	return new, clean
}

func (m *RegionManager) readQ() {
	for rid := range m.readChan {
		m.sketchMu.Lock()
		m.sketch.Add(spread(uint64(rid)))
		m.sketchMu.Unlock()
	}
}
