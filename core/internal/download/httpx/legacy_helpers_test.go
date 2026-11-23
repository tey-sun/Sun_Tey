package httpx

// Legacy (pre-M1) tests pinned the static "one chunk per connection" layout.
// smallSplits restores exactly that plan by shrinking the M1 knobs to the old
// behaviour: every byte range >= 64 KiB splits, one segment per leg. The
// production defaults (4 MiB min-split, 1 MiB floor, 2 segments per leg) are
// covered by dynamic_test.go instead.
func smallSplits(d *Downloader) *Downloader {
	d.MinSplitSize = 64 << 10
	d.SegmentFloor = 64 << 10
	d.SegmentsPerLeg = 1
	return d
}
