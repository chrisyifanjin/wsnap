package engine

import (
	"slices"
	"testing"
	"time"
)

// TestRetentionKeepsDailyHistory (reviewer #4 finding 3): a burst of new
// snapshots must not push every older day out of history.
func TestRetentionKeepsDailyHistory(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	day := func(d, h int) time.Time { return time.Date(2026, 11, d, h, 0, 0, 0, time.UTC) }
	snaps := []snapRef{
		{"old-1", day(1, 1)}, {"nov4-a", day(4, 1)}, {"nov4-b", day(4, 2)}, {"nov6", day(6, 9)},
	}
	// An attacker's burst today: 30 snapshots.
	for i := range 30 {
		snaps = append(snaps, snapRef{id: "burst-" + string(rune('a'+i)), t: day(7, 0).Add(time.Duration(i) * time.Minute)})
	}
	got := forgettable(snaps, Retention{KeepLast: 10, KeepDaily: 7, Keep: []string{"old"}}, now)
	for _, mustKeep := range []string{"nov4-b", "nov6", "old-1"} {
		if slices.Contains(got, mustKeep) {
			t.Errorf("%s deleted; the daily floor (or the pin) must keep it", mustKeep)
		}
	}
	if !slices.Contains(got, "nov4-a") {
		t.Error("nov4-a kept; only the newest of a day stays")
	}
	if n := len(snaps) - len(got); n != 10+3 {
		t.Errorf("kept %d snapshots, want 10 newest + nov4-b + nov6 + the pinned old-1 = 13", n)
	}
	// Outside the window, only keepLast and pins hold.
	if got := forgettable(snaps, Retention{KeepLast: 10, KeepDaily: 2}, now); !slices.Contains(got, "nov4-b") || slices.Contains(got, "nov6") {
		t.Errorf("keepDaily=2 kept the wrong days: deleted %v", got)
	}
}
