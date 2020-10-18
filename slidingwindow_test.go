package slidingwindow

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

const (
	d     = 100 * time.Millisecond
	size  = time.Second
	limit = int64(10)
)

var (
	t0  = time.Now().Truncate(size)
	t1  = t0.Add(1 * d)
	t2  = t0.Add(2 * d)
	t3  = t0.Add(3 * d)
	t4  = t0.Add(4 * d)
	t5  = t0.Add(5 * d)
	t6  = t0.Add(6 * d)
	t7  = t0.Add(7 * d)
	t8  = t0.Add(8 * d)
	t9  = t0.Add(9 * d)
	t10 = t0.Add(10 * d)
	t11 = t0.Add(11 * d)
	t12 = t0.Add(12 * d)
	t13 = t0.Add(13 * d)
	t14 = t0.Add(14 * d)
	t15 = t0.Add(15 * d)
	t16 = t0.Add(16 * d)
	t17 = t0.Add(17 * d)
	t18 = t0.Add(18 * d)
	t19 = t0.Add(19 * d)
	t20 = t0.Add(20 * d)
	t21 = t0.Add(21 * d)
	t22 = t0.Add(22 * d)
	t23 = t0.Add(23 * d)
	t24 = t0.Add(24 * d)
	t25 = t0.Add(25 * d)
	t26 = t0.Add(26 * d)
	t27 = t0.Add(27 * d)
	t28 = t0.Add(28 * d)
	t29 = t0.Add(29 * d)
	t30 = t0.Add(30 * d)
	t31 = t0.Add(31 * d)
	t32 = t0.Add(32 * d)
)

type caseArg struct {
	t  time.Time
	n  int64
	ok bool
}

func TestLimiter_LocalWindow_SetLimit(t *testing.T) {
	lim, _ := NewLimiter(size, limit, func() (Window, StopFunc) {
		return NewLocalWindow()
	})

	got := lim.Limit()
	if got != limit {
		t.Errorf("lim.Limit() = %d, want: %d", got, limit)
	}

	newLimit := int64(12)
	lim.SetLimit(newLimit)
	got = lim.Limit()
	if got != newLimit {
		t.Errorf("lim.Limit() = %d, want: %d", got, newLimit)
	}
}

func TestLimiter_LocalWindow_AllowN(t *testing.T) {
	lim, _ := NewLimiter(size, limit, func() (Window, StopFunc) {
		return NewLocalWindow()
	})

	cases := []caseArg{
		// prev-window: empty, count: 0
		// curr-window: [t0, t0 + 1s), count: 0
		{t0, 1, true},
		{t1, 2, true},
		{t2, 3, true},
		{t5, 5, false}, // count will be (1 + 2 + 3 + 5) = 11, so it fails

		// prev-window: [t0, t0 + 1s), count: 6
		// curr-window: [t10, t10 + 1s), count: 0
		{t10, 2, true},
		{t12, 5, false}, // count will be (4/5*6 + 2 + 5) ≈ 11, so it fails
		{t15, 5, true},

		// prev-window: [t30 - 1s, t30), count: 0
		// curr-window: [t30, t30 + 1s), count: 0
		{t30, 10, true},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			ok := lim.AllowN(c.t, c.n)
			if ok != c.ok {
				t.Errorf("lim.AllowN(%v, %v) = %v, want: %v",
					c.t, c.n, ok, c.ok)
			}
		})
	}
}

func TestLimiter_LocalWindow_AllowOne(t *testing.T) {
	lim, _ := NewLimiter(size, 1, func() (Window, StopFunc) {
		return NewLocalWindow()
	})

	cases := []caseArg{
		// prev-window: empty, count: 0
		// curr-window: [t0, t0 + 1s), count: 0
		{t0, 1, true},
		{t1, 1, false},
		{t2, 1, false},
		{t3, 1, false},
		{t4, 1, false},
		// prev-window: [t0, t0 + 1s), count: 1
		// curr-window: [t10, t10 + 1s), count: 0
		{t10, 1, true},
		{t11, 1, false},
		{t12, 1, false},
		{t13, 1, false},
		{t14, 1, false},

		// prev-window: [t0, t0 + 1s), count: 1
		// curr-window: [t10, t10 + 1s), count: 0
		{t20, 1, true},
		{t12, 1, false},
		{t15, 1, false},

		// prev-window: [t30 - 1s, t30), count: 0
		// curr-window: [t30, t30 + 1s), count: 0
		{t30, 1, true},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			ok := lim.AllowOne(c.t)
			if ok != c.ok {
				t.Errorf("lim.AllowN(%v, %v) = %v, want: %v",
					c.t, c.n, ok, c.ok)
			}
		})
	}
}

func TestLimiter_LocalWindow_AllowOne_2_1(t *testing.T) {
	lim, _ := NewLimiter(size, 2, func() (Window, StopFunc) {
		return NewLocalWindow()
	})

	cases := []caseArg{
		// prev-window: empty, count: 0
		// curr-window: [t0, t0 + 1s), count: 0
		{t0, 1, true},
		{t1, 1, true},
		{t2, 1, false},
		{t3, 1, false},
		{t4, 1, false},
		// prev-window: [t0, t0 + 1s), count: 2
		// curr-window: [t10, t10 + 1s), count: 0
		{t10, 1, false},
		{t11, 1, false},
		{t12, 1, false},
		{t13, 1, false},
		{t14, 1, false},
		{t15, 1, true},
		{t16, 1, true},
		{t17, 1, false},
		{t18, 1, false},
		{t19, 1, false},

		// prev-window: [t0, t0 + 1s), count: 5
		// curr-window: [t10, t10 + 1s), count: 0
		// {t20, 1, false},
		// {t12, 1, false},
		// {t13, 1, false},
		// {t14, 1, false},
		// {t15, 1, true},
		// {t16, 1, true},
		// {t17, 1, false},

		// prev-window: [t30 - 1s, t30), count: 0
		// curr-window: [t30, t30 + 1s), count: 0
		{t30, 1, true},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			ok := lim.AllowOne(c.t)
			if ok != c.ok {
				t.Errorf("lim.AllowN(%v, %v) = %v, want: %v",
					c.t, c.n, ok, c.ok)
			}
		})
	}
}

func TestLimiter_LocalWindow_AllowOne_2_2(t *testing.T) {
	lim, _ := NewLimiter(size, 2, func() (Window, StopFunc) {
		return NewLocalWindow()
	})

	cases := []caseArg{
		// prev-window: empty, count: 0
		// curr-window: [t0, t0 + 1s), count: 0
		{t0, 1, true},
		// {t1, 1, false},
		// {t2, 1, false},
		// {t3, 1, false},
		// {t4, 1, false},
		// prev-window: [t0, t0 + 1s), count: 2
		// curr-window: [t10, t10 + 1s), count: 0
		{t10, 1, true},
		{t11, 1, true},
		{t12, 1, false},
		{t13, 1, false},
		{t14, 1, false},
		{t15, 1, false},
		{t16, 1, false},
		{t17, 1, false},
		{t18, 1, false},
		{t19, 1, false},

		// prev-window: [t0, t0 + 1s), count: 5
		// curr-window: [t10, t10 + 1s), count: 0
		// {t20, 1, false},
		// {t12, 1, false},
		// {t13, 1, false},
		// {t14, 1, false},
		// {t15, 1, true},
		// {t16, 1, true},
		// {t17, 1, false},

		// prev-window: [t30 - 1s, t30), count: 0
		// curr-window: [t30, t30 + 1s), count: 0
		{t30, 1, true},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			ok := lim.AllowOne(c.t)
			if ok != c.ok {
				t.Errorf("lim.AllowN(%v, %v) = %v, want: %v",
					c.t, c.n, ok, c.ok)
			}
		})
	}
}

// LocalWindow represents a window that ignores sync behavior entirely
// and only stores counters in memory.
type ManualSyncLocalWindow struct {
	LocalWindow
	oneSideCount int64
	lastSynced   time.Time
}

func (w *ManualSyncLocalWindow) Sync(now time.Time) {
	if now.Sub(w.lastSynced) >= time.Millisecond*200 {
		w.count = w.oneSideCount * 2
		w.lastSynced = now
	}
}

func (w *ManualSyncLocalWindow) AddCount(n int64) {
	w.oneSideCount += n
	w.count += n
}

func (w *ManualSyncLocalWindow) Reset(s time.Time, c int64) {
	w.start = s.UnixNano()
	w.count = c
	w.oneSideCount = c
}

func TestLimiter_LocalWindow_AllowOne_2_3_simulateSync(t *testing.T) {
	lim, _ := NewLimiter(size, 10, func() (Window, StopFunc) {
		return &ManualSyncLocalWindow{}, func() {}
	})

	cases := []caseArg{
		// prev-window: empty, count: 0
		// curr-window: [t0, t0 + 1s), count: 0
		{t0, 1, true},  // #0 sync
		{t1, 1, true},  // #1
		{t2, 1, true},  // #2 also trigger Sync, 6
		{t3, 1, true},  // #3 7
		{t3, 1, true},  // #4
		{t3, 1, true},  // #5
		{t4, 1, true},  // #6 trigger Sync, 7 each
		{t6, 1, false}, // #7
		{t7, 1, false}, // #8

		// prev-window: [t0, t0 + 1s), count: 14
		// curr-window: [t10, t10 + 1s), count: 0
		{t10, 1, false}, // #9 sync, slow fail
		{t11, 1, false}, // #10 quick fail
		{t12, 1, false}, // #11 quick fail
		{t13, 1, false}, // #12 quick fail
		{t14, 1, true},  // #13 sync, 2
		{t14, 1, false}, // #14 try sync, slow fail
		{t15, 1, true},  // #15 3
		{t15, 1, false}, // #16 quick fail
		{t16, 1, true},  // #17 sync, 6
		{t16, 1, false}, // #18 try sync, slow fail
		{t17, 1, false}, // #19 quick fail
		{t18, 1, true},  // #20 sync, 8
		{t19, 1, false}, // #21 try sync, slow fail
		// prev-window: [t0, t0 + 1s), count: 8
		// curr-window: [t10, t10 + 1s), count: 0
		{t20, 1, true},  // #22 sync, 2, quick succeed, 1
		{t20, 1, false}, // #23 try sync, quick succeed, 2
		{t20, 1, false}, // #24 try sync, slow fail
		{t21, 1, false}, // #25 quick fail
		{t22, 1, true},  // #26 sync, 4
		{t22, 1, false}, // #27 try sync, slow fail
		{t23, 1, false}, // #28 quick fail
		{t24, 1, true},  // #29 sync, 6
		{t25, 1, false}, // #30 quick fail
		{t26, 1, false}, // #31 quick fail
		{t27, 1, true},  // #32 sync, 8
		{t27, 1, false}, // #33 try sync, slow fail
		{t28, 1, false}, // #34 quick fail
		{t29, 1, true},  // #35 sync, 10
		{t29, 1, false}, // #36 try sync, slow fail

		// prev-window: [t30 - 1s, t30), count: 10
		// curr-window: [t30, t30 + 1s), count: 0
		{t30, 1, false}, // #37 try sync, slow fail
		{t31, 1, true},  // #38 sync, 2
	}

	for i, c := range cases {
		index := i
		_ = index

		t.Run("", func(t *testing.T) {
			ok := lim.AllowOne(c.t)
			du := time.Unix(0, lim.nextAllowTime.Load()).Sub(lim.curr.Start())
			// t.Logf("curr.count: %v, nextT: %s", lim.curr.Count(), du)
			if ok != c.ok {
				t.Errorf("lim.AllowN(%v, %v) = %v, want: %v, curr.count: %v, nextT: %s",
					c.t, c.n, ok, c.ok, lim.curr.Count(), du)
			}
		})
	}
}

type MemDatastore struct {
	data map[string]int64
	mu   sync.RWMutex
}

func newMemDatastore() *MemDatastore {
	return &MemDatastore{
		data: make(map[string]int64),
	}
}

func (d *MemDatastore) fullKey(key string, start int64) string {
	return fmt.Sprintf("%s@%d", key, start)
}

func (d *MemDatastore) Add(key string, start, delta int64) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	k := d.fullKey(key, start)
	d.data[k] += delta
	return d.data[k], nil
}

func (d *MemDatastore) Get(key string, start int64) (int64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	k := d.fullKey(key, start)
	return d.data[k], nil
}

func testSyncWindow(t *testing.T, blockingSync bool, cases []caseArg) {
	store := newMemDatastore()
	newWindow := func() (Window, StopFunc) {
		// Sync will happen every 200ms (syncInterval), but for test purpose,
		// we check at the 600ms boundaries (i.e. t6 and t16, see cases below).
		//
		// The reason is that:
		//
		//     - We need to wait for at least 400ms (twice of syncInterval) to
		//       ensure that the two parallel limiters have a consistent view of
		//       the count after two times of exchange with the central datastore.
		//       See the examples below for clarification.
		//
		//     - The parallel test runner sometimes might cause delays, so we
		//       wait another 200ms since the last sync.
		//
		// e.g.:
		//
		//     [Initial]
		//         lim1 (count: 0)                   datastore (count: 0)
		//         lim2 (count: 0)                   datastore (count: 0)
		//
		//     [Allow_1]
		//         lim1 (count: 1)                   datastore (count: 0)
		//         lim2 (count: 1)                   datastore (count: 0)
		//
		//     [1st Sync] -- inconsistent
		//         lim1 (count: 1)  -- add-req  -->  datastore (count: 0)
		//         lim1 (count: 1)  <-- add-resp --  datastore (count: 1)
		//         lim2 (count: 1)  -- add-req  -->  datastore (count: 1)
		//         lim2 (count: 2)  <-- add-resp --  datastore (count: 2)
		//
		//     [2nd Sync] -- consistent
		//         lim1 (count: 1)  -- get-req  -->  datastore (count: 2)
		//         lim1 (count: 2)  <-- get-resp --  datastore (count: 2)
		//         lim2 (count: 2)  -- get-req  -->  datastore (count: 2)
		//         lim2 (count: 2)  <-- get-resp --  datastore (count: 2)
		//
		// Also note that one synchronization is driven by one call to `Sync()`
		// in blocking-sync mode, while in non-blocking-sync mode, it is driven
		// by two calls to `Sync()`.
		//
		var syncer Synchronizer

		syncInterval := 200 * time.Millisecond

		if blockingSync {
			syncer = NewBlockingSynchronizer(store, syncInterval)
		} else {
			syncer = NewNonblockingSynchronizer(store, syncInterval)
		}
		return NewSyncWindow("test", syncer)
	}

	parallelisms := []struct {
		name  string
		cases []caseArg
	}{
		{
			"parallel-lim1",
			cases,
		},
		{
			"parallel-lim2",
			cases,
		},
	}
	for _, p := range parallelisms {
		p := p
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()

			lim, stop := NewLimiter(size, limit, newWindow)

			prevT := t0
			for _, c := range p.cases {
				t.Run("", func(t *testing.T) {
					// Wait the given duration to keep in step with the
					// other parallel test.
					time.Sleep(c.t.Sub(prevT))
					prevT = c.t

					runtime.Gosched()
					ok := lim.AllowN(c.t, c.n)
					runtime.Gosched()
					if ok != c.ok {
						t.Errorf("lim.AllowN(%v, %v) = %v, want: %v",
							c.t, c.n, ok, c.ok)
					}
				})
			}

			// NOTE: Calling stop() at the top-level of TestLimiter_SyncWindow_AllowN
			// will break. See https://github.com/golang/go/issues/17791 for details.
			stop()
		})
	}
}

func triggerSync(t time.Time) caseArg {
	// Construct a failure case at time t, simply for triggering the sync
	// behaviour of the limiter's window.
	return caseArg{t, limit + 1, false}
}

func TestLimiter_Blocking_SyncWindow_AllowN(t *testing.T) {
	testSyncWindow(t, true, []caseArg{
		// prev-window: empty, count: 0
		// curr-window: [t0, t0 + 1s), count: 0
		{t0, 1, true},
		{t1, 1, true},
		{t2, 1, true}, // also trigger Sync
		triggerSync(t4),
		{t6, 5, false}, // reach consistent: count will be (2*1 + 2*1 + 2*1 + 5) = 11, so it fails

		// prev-window: [t0, t0 + 1s), count: 6
		// curr-window: [t10, t10 + 1s), count: 0
		{t10, 2, true},
		{t12, 5, false}, // also trigger Sync: count will be (4/5*6 + 2 + 5) ≈ 11, so it fails
		triggerSync(t14),
		{t16, 5, false}, // reach consistent: count will be (2/5*6 + 2*2 + 5) ≈ 11, so it fails
		{t18, 5, true},

		// prev-window: [t30 - 1s, t30), count: 0
		// curr-window: [t30, t30 + 1s), count: 0
		{t30, 10, true},
	})
}

func TestLimiter_Nonblocking_SyncWindow_AllowN(t *testing.T) {
	testSyncWindow(t, false, []caseArg{
		// prev-window: empty, count: 0
		// curr-window: [t0, t0 + 1s), count: 0
		{t0, 1, true},
		{t1, 1, true},   // 100ms
		{t2, 1, true},   // also trigger Sync, 200ms
		triggerSync(t3), // 300ms
		triggerSync(t4), // 400ms
		triggerSync(t4), // 700ms
		{t5, 5, false},  // reach consistent: count will be (2*1 + 2*1 + 2*1 + 5) = 11, so it fails, //800ms

		// prev-window: [t0, t0 + 1s), count: 6
		// curr-window: [t10, t10 + 1s), count: 0
		{t10, 2, true},
		{t12, 5, false}, // also trigger Sync: count will be (4/5*6 + 2 + 5) ≈ 11, so it fails
		triggerSync(t13),
		triggerSync(t14),
		triggerSync(t14),
		{t16, 5, false}, // reach consistent: count will be (2/5*6 + 2*2 + 5) ≈ 11, so it fails
		{t18, 5, true},

		// prev-window: [t30 - 1s, t30), count: 0
		// curr-window: [t30, t30 + 1s), count: 0
		{t30, 10, true},
	})
}
