package player

// Regression test for the gapless-swap race (issue 1.1).
//
// The bug: gaplessStreamer.Stream used to fire the onSwap callback on a
// detached goroutine (`go swapFn()`). The detached swap ran with no
// coordination against the UI thread, which could land a new track via
// playPipeline/preloadPipeline in the window between the audio thread
// promoting the next stream and the detached swap committing p.current. The
// late swap then clobbered p.current and wrongly closed the freshly-selected
// track.
//
// This test forces exactly that interleaving: track A drains (transition
// fires, swap scheduled), then a new track C is installed before the swap runs.
// If the swap ever runs asynchronously again, it commits after the UI thread's
// change and clobbers p.current — the test fails. With the synchronous swap
// (committed inside Stream, under the speaker lock) the bookkeeping lands
// before the UI thread can act, so nothing is clobbered.
//
// The bookkeeping itself is driven through the real Player.gaplessSwap method;
// the completion channel only tells the test when a (possibly detached) swap
// has finished running, so the assertions see a quiesced player in both cases.

import (
	"sync/atomic"
	"testing"

	"github.com/gopxl/beep/v2"
)

// gaplessTestStreamer is a beep.StreamSeekCloser that drains immediately
// (drain=true) to trigger a gapless transition, and counts Close() calls so we
// can detect a decoder closed while it should still be in use.
type gaplessTestStreamer struct {
	drain  bool
	closes atomic.Int32
}

func (s *gaplessTestStreamer) Stream(samples [][2]float64) (int, bool) {
	if s.drain {
		return 0, false
	}
	return len(samples), true
}

func (s *gaplessTestStreamer) Err() error       { return nil }
func (s *gaplessTestStreamer) Len() int         { return 0 }
func (s *gaplessTestStreamer) Position() int    { return 0 }
func (s *gaplessTestStreamer) Seek(p int) error { return nil }
func (s *gaplessTestStreamer) Close() error     { s.closes.Add(1); return nil }

var _ beep.StreamSeekCloser = (*gaplessTestStreamer)(nil)

func gaplessTestPipe(s *gaplessTestStreamer) *trackPipeline {
	return &trackPipeline{decoder: s, stream: s}
}

func TestGaplessSwapDoesNotClobberNewTrack(t *testing.T) {
	p := newTestPlayer()
	p.gapless = &gaplessStreamer{}

	const iterations = 25
	for i := 0; i < iterations; i++ {
		p.gaplessAdvance.Store(false)

		a := &gaplessTestStreamer{drain: true} // currently playing, ends immediately
		b := &gaplessTestStreamer{}            // preloaded next track
		c := &gaplessTestStreamer{}            // track the UI just selected

		swapDone := make(chan struct{})
		p.gapless.onSwap = func() {
			p.gaplessSwap()
			close(swapDone)
		}

		p.gapless.Replace(a)
		p.gapless.SetNext(b)
		p.mu.Lock()
		p.current = gaplessTestPipe(a)
		p.nextPipeline = gaplessTestPipe(b)
		p.mu.Unlock()

		// Audio thread: track A ends, gapless promotes B and commits the swap.
		p.gapless.Stream(make([][2]float64, 1024))

		// UI thread (mirrors playPipeline, player.go:179-216): the main
		// goroutine runs this without yielding. If the swap is synchronous it
		// has already committed and this is a clean supersede; if the swap is
		// (re)introduced asynchronously it lands in the gap and clobbers below.
		p.mu.Lock()
		oldCur, oldNext := p.current, p.nextPipeline
		p.current = gaplessTestPipe(c)
		p.nextPipeline = nil
		p.mu.Unlock()
		go closePipelines(oldCur, oldNext)

		// Park until any swap has finished, so the assertions see a quiesced
		// player in both the synchronous and asynchronous cases.
		<-swapDone

		if p.current == nil || p.current.decoder != c {
			got := "<nil>"
			if p.current != nil {
				got = "some other pipeline"
			}
			t.Fatalf(
				"iteration %d: p.current clobbered by late gapless swap: got %s (want the freshly selected track c)",
				i, got,
			)
		}
		if n := c.closes.Load(); n != 0 {
			t.Fatalf("iteration %d: freshly-selected track c was closed %d time(s) by the stale swap", i, n)
		}
	}
}
