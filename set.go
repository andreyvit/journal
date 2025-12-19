package journal

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"
)

type SetOptions struct {
	Now             func() time.Time
	Logger          *slog.Logger
	AutosealEnabled bool
	AutosealDelay   time.Duration
}

type Set struct {
	now    func() time.Time
	logger *slog.Logger

	lock      sync.Mutex
	_journals []*Journal

	autosealEnabled bool
	autosealDelay   time.Duration
}

type SetRunner struct {
	set      *Set
	shutdown context.CancelFunc
	wg       sync.WaitGroup
}

func NewSet(opt SetOptions) *Set {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	return &Set{
		now:             opt.Now,
		logger:          opt.Logger,
		autosealEnabled: opt.AutosealEnabled,
		autosealDelay:   opt.AutosealDelay,
	}
}

func (set *Set) Add(j *Journal) {
	set.lock.Lock()
	defer set.lock.Unlock()
	set._journals = append(set._journals, j)
}

func (set *Set) Remove(j *Journal) {
	set.lock.Lock()
	defer set.lock.Unlock()
	i := slices.Index(set._journals, j)
	if i != -1 {
		set._journals = slices.Delete(set._journals, i, i+1)
	}
}

func (set *Set) Journals() []*Journal {
	set.lock.Lock()
	defer set.lock.Unlock()
	return slices.Clone(set._journals)
}

func (set *Set) Process(ctx context.Context) int {
	return set.Autocommit(ctx) + set.Autoseal(ctx)
}

func (set *Set) Autocommit(ctx context.Context) int {
	journals := set.Journals()
	now := ToTimestamp(set.now())
	var actions int
	for _, j := range journals {
		if ctx.Err() != nil {
			return actions
		}
		ok, err := j.Autocommit(now)
		if err != nil {
			j.logger.Error("commit error", "err", err)
			continue
		} else if ok {
			actions++
		}
		ok, err = j.Autorotate(now)
		if err != nil {
			j.logger.Error("autorotate error", "err", err)
		} else if ok {
			j.logger.Debug("autorotated", "journal", j.String())
			actions++
		}
	}
	return actions
}

func (set *Set) Autoseal(ctx context.Context) int {
	journals := set.Journals()
	var actions int
	for _, j := range journals {
		if ctx.Err() != nil {
			return actions
		}
		n, err := j.SealAndTrimOnce(ctx)
		actions += n
		if err != nil {
			j.logger.Error("seal/trim error", "err", err)
			continue
		}
		if n > 0 && set.autosealDelay > 0 {
			time.Sleep(set.autosealDelay)
		}
	}
	return actions
}

func (set *Set) StartBackground(ctx context.Context) *SetRunner {
	ctx, cancel := context.WithCancel(ctx)
	runner := &SetRunner{
		set:      set,
		shutdown: cancel,
	}
	runner.wg.Add(1)
	go runPeriodical(ctx, &runner.wg, set.Autocommit, time.Second)
	if set.autosealEnabled {
		runner.wg.Add(1)
		go runPeriodical(ctx, &runner.wg, set.Autoseal, 2*time.Second)
	}
	return runner
}

func (runner *SetRunner) Close() {
	runner.shutdown()
	runner.wg.Wait()
}

func runPeriodical(ctx context.Context, wg *sync.WaitGroup, f func(ctx context.Context) int, interval time.Duration) {
	defer wg.Done()
	for {
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
			// nop -- run again
		case <-ctx.Done():
			timer.Stop()
			return
		}
		f(ctx)
	}
}
