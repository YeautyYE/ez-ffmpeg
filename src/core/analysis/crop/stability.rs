//! Per-stream temporal state: skip, bootstrap, hysteresis, reset, scene cut.

use super::luma::ChromaGrid;
use super::scan::align_outward;
use super::{CropObservation, CropRawBounds, CropSuggestion};
use crate::core::analysis::event::Timestamp;

const MAX_CANDIDATES: usize = 120;
const BOOTSTRAP_NEEDED: usize = 3;
const INWARD_MAJORITY: f32 = 0.70;
const INWARD_HOLD_US: i64 = 200_000;
const DEFAULT_WINDOW_US: i64 = 500_000;
const NO_PTS_WINDOW: usize = 15;

#[derive(Clone, Copy)]
struct Slot {
    raw: CropRawBounds,
    time_us: Option<i64>,
    occupied: bool,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            raw: CropRawBounds {
                left: 0,
                top: 0,
                right_exclusive: 0,
                bottom_exclusive: 0,
            },
            time_us: None,
            occupied: false,
        }
    }
}

pub(crate) struct Stability {
    slots: [Slot; MAX_CANDIDATES],
    head: usize,
    len: usize,
    skip_remaining: u32,
    evaluated_in_epoch: u64,
    reset_every: u32,
    pending_reset: bool,
    stable: Option<CropRawBounds>,
    last_pts_us: Option<i64>,
    window_us: i64,
    frame_w: i32,
    frame_h: i32,
    round: u32,
    chroma: ChromaGrid,
    evaluated_total: u64,
}

impl Stability {
    pub(crate) fn new(skip_initial: u32, reset_every: u32, window_us: i64, round: u32) -> Self {
        Self {
            slots: [Slot::default(); MAX_CANDIDATES],
            head: 0,
            len: 0,
            skip_remaining: skip_initial,
            evaluated_in_epoch: 0,
            reset_every,
            pending_reset: false,
            stable: None,
            last_pts_us: None,
            window_us: if window_us > 0 {
                window_us
            } else {
                DEFAULT_WINDOW_US
            },
            frame_w: 0,
            frame_h: 0,
            round,
            chroma: ChromaGrid::None,
            evaluated_total: 0,
        }
    }

    pub(crate) fn set_geometry(&mut self, w: i32, h: i32, chroma: ChromaGrid) {
        if self.frame_w != 0 && (self.frame_w != w || self.frame_h != h || self.chroma != chroma) {
            self.clear_evidence();
            self.stable = None;
        }
        self.frame_w = w;
        self.frame_h = h;
        self.chroma = chroma;
    }

    pub(crate) fn skip_due(&self) -> bool {
        self.skip_remaining > 0
    }

    pub(crate) fn consume_skip(&mut self) {
        if self.skip_remaining > 0 {
            self.skip_remaining -= 1;
        }
    }

    pub(crate) fn on_evaluated_frame(&mut self) {
        self.evaluated_total = self.evaluated_total.saturating_add(1);
        self.evaluated_in_epoch = self.evaluated_in_epoch.saturating_add(1);
    }

    pub(crate) fn maybe_periodic_reset(&mut self) {
        if self.reset_every == 0 {
            return;
        }
        if self.evaluated_in_epoch < self.reset_every as u64 {
            return;
        }
        if self.stable.is_none() {
            self.pending_reset = true;
            return;
        }
        self.clear_evidence();
        self.evaluated_in_epoch = 0;
        self.pending_reset = false;
    }

    pub(crate) fn reset_scene(&mut self, full: CropRawBounds) {
        self.clear_evidence();
        self.stable = Some(full);
        self.evaluated_in_epoch = 0;
        self.pending_reset = false;
    }

    pub(crate) fn clear_evidence(&mut self) {
        self.len = 0;
        self.head = 0;
        for slot in &mut self.slots {
            slot.occupied = false;
        }
    }

    pub(crate) fn observe(&mut self, raw: CropRawBounds, time_us: Option<i64>) {
        if let (Some(prev), Some(now)) = (self.last_pts_us, time_us) {
            if now < prev {
                self.clear_evidence();
            }
        }
        self.last_pts_us = time_us.or(self.last_pts_us);

        if self.len == MAX_CANDIDATES {
            let drop_at = (self.head + MAX_CANDIDATES - self.len) % MAX_CANDIDATES;
            self.slots[drop_at].occupied = false;
            self.len -= 1;
        }
        self.slots[self.head] = Slot {
            raw,
            time_us,
            occupied: true,
        };
        self.head = (self.head + 1) % MAX_CANDIDATES;
        self.len += 1;

        if self.stable.is_none() {
            if self.count_in_window(time_us) >= BOOTSTRAP_NEEDED {
                // Independent per-edge median, not the outward union: one
                // wide outlier must not own the first published rectangle.
                let snap = self.snapshot_window(time_us);
                let fallback = CropRawBounds {
                    left: 0,
                    top: 0,
                    right_exclusive: self.frame_w,
                    bottom_exclusive: self.frame_h,
                };
                self.stable = Some(consensus(&snap, fallback));
                if self.pending_reset {
                    self.clear_evidence();
                    self.evaluated_in_epoch = 0;
                    self.pending_reset = false;
                }
            }
            return;
        }

        let snap = self.snapshot_window(time_us);
        let stable = self.stable.unwrap();
        // A single high-confidence candidate may expand the box immediately.
        let expanded = outward_union(stable, raw);
        let proposed = consensus(&snap, expanded);
        let shrinking = proposed.left > expanded.left
            || proposed.top > expanded.top
            || proposed.right_exclusive < expanded.right_exclusive
            || proposed.bottom_exclusive < expanded.bottom_exclusive;
        if shrinking && !is_outward(raw, stable) && inward_allowed(expanded, proposed, &snap) {
            self.stable = Some(proposed);
        } else {
            self.stable = Some(expanded);
        }
    }

    pub(crate) fn current_aligned(&self) -> Option<(CropSuggestion, CropObservation)> {
        let raw = self.stable?;
        if self.frame_w <= 0 || self.frame_h <= 0 {
            return None;
        }
        let aligned = align_outward(raw, self.frame_w, self.frame_h, self.round, self.chroma);
        let suggestion = CropSuggestion {
            x: aligned.left,
            y: aligned.top,
            w: aligned.right_exclusive - aligned.left,
            h: aligned.bottom_exclusive - aligned.top,
        };
        let at = Timestamp {
            time_us: self.last_pts_us.unwrap_or(0),
            pts: self.last_pts_us,
            time_base: None,
        };
        Some((
            suggestion,
            CropObservation {
                at,
                raw,
                aligned: suggestion,
            },
        ))
    }

    #[cfg(test)]
    pub(crate) fn has_stable(&self) -> bool {
        self.stable.is_some()
    }

    #[cfg(test)]
    pub(crate) fn evaluated_total(&self) -> u64 {
        self.evaluated_total
    }

    fn snapshot_window(&self, now: Option<i64>) -> WindowSnapshot {
        let mut snap = WindowSnapshot::empty();
        if self.len == 0 {
            return snap;
        }
        let start = (self.head + MAX_CANDIDATES - self.len) % MAX_CANDIDATES;
        for i in 0..self.len {
            let slot = self.slots[(start + i) % MAX_CANDIDATES];
            if !slot.occupied {
                continue;
            }
            if let (Some(now_us), Some(t)) = (now, slot.time_us) {
                if now_us.saturating_sub(t) > self.window_us {
                    continue;
                }
            }
            snap.push(slot.raw, slot.time_us);
        }
        if now.is_none() && snap.len > NO_PTS_WINDOW {
            snap.keep_last(NO_PTS_WINDOW);
        }
        snap
    }

    fn count_in_window(&self, now: Option<i64>) -> usize {
        self.snapshot_window(now).len
    }
}

#[derive(Clone, Copy)]
struct WindowSnapshot {
    items: [CropRawBounds; MAX_CANDIDATES],
    times: [Option<i64>; MAX_CANDIDATES],
    len: usize,
}

impl WindowSnapshot {
    fn empty() -> Self {
        Self {
            items: [CropRawBounds {
                left: 0,
                top: 0,
                right_exclusive: 0,
                bottom_exclusive: 0,
            }; MAX_CANDIDATES],
            times: [None; MAX_CANDIDATES],
            len: 0,
        }
    }

    fn push(&mut self, raw: CropRawBounds, time_us: Option<i64>) {
        if self.len >= MAX_CANDIDATES {
            return;
        }
        self.items[self.len] = raw;
        self.times[self.len] = time_us;
        self.len += 1;
    }

    fn keep_last(&mut self, n: usize) {
        if self.len <= n {
            return;
        }
        let drain = self.len - n;
        for i in 0..n {
            self.items[i] = self.items[i + drain];
            self.times[i] = self.times[i + drain];
        }
        self.len = n;
    }
}

fn consensus(snap: &WindowSnapshot, stable: CropRawBounds) -> CropRawBounds {
    if snap.len == 0 {
        return stable;
    }
    let mut left = [0i32; MAX_CANDIDATES];
    let mut top = [0i32; MAX_CANDIDATES];
    let mut right = [0i32; MAX_CANDIDATES];
    let mut bottom = [0i32; MAX_CANDIDATES];
    for i in 0..snap.len {
        left[i] = snap.items[i].left;
        top[i] = snap.items[i].top;
        right[i] = snap.items[i].right_exclusive;
        bottom[i] = snap.items[i].bottom_exclusive;
    }
    let l = median_n(&mut left, snap.len);
    let t = median_n(&mut top, snap.len);
    let r = median_n(&mut right, snap.len);
    let b = median_n(&mut bottom, snap.len);
    CropRawBounds {
        left: l,
        top: t,
        right_exclusive: r.max(l + 1),
        bottom_exclusive: b.max(t + 1),
    }
}

fn median_n(buf: &mut [i32], n: usize) -> i32 {
    if n == 0 {
        return 0;
    }
    let slice = &mut buf[..n];
    slice.sort_unstable();
    slice[n / 2]
}

fn outward_union(a: CropRawBounds, b: CropRawBounds) -> CropRawBounds {
    CropRawBounds {
        left: a.left.min(b.left),
        top: a.top.min(b.top),
        right_exclusive: a.right_exclusive.max(b.right_exclusive),
        bottom_exclusive: a.bottom_exclusive.max(b.bottom_exclusive),
    }
}

fn is_outward(raw: CropRawBounds, stable: CropRawBounds) -> bool {
    raw.left < stable.left
        || raw.top < stable.top
        || raw.right_exclusive > stable.right_exclusive
        || raw.bottom_exclusive > stable.bottom_exclusive
}

fn inward_allowed(stable: CropRawBounds, proposed: CropRawBounds, snap: &WindowSnapshot) -> bool {
    if snap.len == 0 {
        return false;
    }
    let shrink_left = proposed.left > stable.left;
    let shrink_top = proposed.top > stable.top;
    let shrink_right = proposed.right_exclusive < stable.right_exclusive;
    let shrink_bottom = proposed.bottom_exclusive < stable.bottom_exclusive;
    if !shrink_left && !shrink_top && !shrink_right && !shrink_bottom {
        return false;
    }
    // Each shrinking edge is voted independently. Coupling all four sides
    // would let one conservative edge veto a well-supported opposite crop.
    if shrink_left && !edge_inward_supported(snap, |r| r.left >= proposed.left) {
        return false;
    }
    if shrink_top && !edge_inward_supported(snap, |r| r.top >= proposed.top) {
        return false;
    }
    if shrink_right
        && !edge_inward_supported(snap, |r| r.right_exclusive <= proposed.right_exclusive)
    {
        return false;
    }
    if shrink_bottom
        && !edge_inward_supported(snap, |r| r.bottom_exclusive <= proposed.bottom_exclusive)
    {
        return false;
    }
    true
}

fn edge_inward_supported(snap: &WindowSnapshot, supports: impl Fn(CropRawBounds) -> bool) -> bool {
    let mut support = 0usize;
    let mut min_t: Option<i64> = None;
    let mut max_t: Option<i64> = None;
    let mut timed_support = 0usize;
    for i in 0..snap.len {
        if !supports(snap.items[i]) {
            continue;
        }
        support += 1;
        if let Some(t) = snap.times[i] {
            timed_support += 1;
            min_t = Some(min_t.map_or(t, |m| m.min(t)));
            max_t = Some(max_t.map_or(t, |m| m.max(t)));
        }
    }
    if support as f32 / (snap.len as f32) < INWARD_MAJORITY {
        return false;
    }
    if timed_support > 0 {
        matches!((min_t, max_t), (Some(lo), Some(hi)) if hi.saturating_sub(lo) >= INWARD_HOLD_US)
    } else {
        snap.len >= 6
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(l: i32, t: i32, r: i32, b: i32) -> CropRawBounds {
        CropRawBounds {
            left: l,
            top: t,
            right_exclusive: r,
            bottom_exclusive: b,
        }
    }

    fn stab() -> Stability {
        let mut s = Stability::new(0, 0, DEFAULT_WINDOW_US, 1);
        s.set_geometry(320, 240, ChromaGrid::None);
        s
    }

    #[test]
    fn skip_does_not_evaluate() {
        let mut s = Stability::new(2, 0, DEFAULT_WINDOW_US, 1);
        assert!(s.skip_due());
        s.consume_skip();
        assert!(s.skip_due());
        s.consume_skip();
        assert!(!s.skip_due());
        assert_eq!(s.evaluated_total(), 0);
    }

    #[test]
    fn bootstrap_needs_three() {
        let mut s = stab();
        let r = raw(0, 40, 320, 200);
        s.observe(r, Some(0));
        assert!(!s.has_stable());
        s.observe(r, Some(40_000));
        assert!(!s.has_stable());
        s.observe(r, Some(80_000));
        assert!(s.has_stable());
        let (sug, _) = s.current_aligned().unwrap();
        assert_eq!(sug.y, 40);
        assert_eq!(sug.h, 160);
    }

    #[test]
    fn bootstrap_uses_independent_median_not_union() {
        let mut s = stab();
        let letter = raw(0, 40, 320, 200);
        s.observe(letter, Some(0));
        s.observe(letter, Some(40_000));
        // One outward full-frame outlier. Union would publish 0..240;
        // independent median of tops {40,40,0} is 40.
        s.observe(raw(0, 0, 320, 240), Some(80_000));
        assert!(s.has_stable());
        let (sug, _) = s.current_aligned().unwrap();
        assert_eq!(sug.y, 40, "outlier must not own bootstrap via union");
        assert_eq!(sug.h, 160);
    }

    #[test]
    fn inward_edges_vote_independently() {
        let mut s = stab();
        let full = raw(0, 0, 320, 240);
        for i in 0..3 {
            s.observe(full, Some(i * 10_000));
        }
        let before = s.current_aligned().unwrap().0;
        // Only the top edge is cropped. Four-edge coupling would reject
        // because left/right/bottom do not move with the candidate.
        let top_only = raw(0, 40, 320, 240);
        for t in (30_000..=230_000).step_by(10_000) {
            s.observe(top_only, Some(t));
        }
        let after = s.current_aligned().unwrap().0;
        assert!(
            after.y > before.y,
            "well-supported top edge must shrink without the other three, before={before:?} after={after:?}"
        );
        assert_eq!(after.x, 0);
        assert_eq!(after.w, 320);
    }

    #[test]
    fn outward_is_immediate() {
        let mut s = stab();
        let small = raw(40, 40, 280, 200);
        for i in 0..3 {
            s.observe(small, Some(i * 40_000));
        }
        let before = s.current_aligned().unwrap().0;
        s.observe(raw(0, 40, 320, 200), Some(200_000));
        let after = s.current_aligned().unwrap().0;
        assert!(after.w >= before.w);
        assert_eq!(after.x, 0);
    }

    #[test]
    fn one_unreliable_does_not_shrink() {
        let mut s = stab();
        let r = raw(0, 40, 320, 200);
        for i in 0..3 {
            s.observe(r, Some(i * 40_000));
        }
        let before = s.current_aligned().unwrap().0;
        // No observe for the black frame — caller skips unreliable frames.
        let after = s.current_aligned().unwrap().0;
        assert_eq!(before, after);
    }

    #[test]
    fn scene_reset_goes_full_frame() {
        let mut s = stab();
        let r = raw(40, 40, 280, 200);
        for i in 0..3 {
            s.observe(r, Some(i * 40_000));
        }
        s.reset_scene(raw(0, 0, 320, 240));
        let sug = s.current_aligned().unwrap().0;
        assert_eq!(sug.x, 0);
        assert_eq!(sug.y, 0);
        assert_eq!(sug.w, 320);
        assert_eq!(sug.h, 240);
    }

    #[test]
    fn reset_one_does_not_block_bootstrap() {
        let mut s = Stability::new(0, 1, DEFAULT_WINDOW_US, 1);
        s.set_geometry(320, 240, ChromaGrid::None);
        let r = raw(0, 20, 320, 220);
        for i in 0..3 {
            s.on_evaluated_frame();
            s.observe(r, Some(i as i64 * 40_000));
            s.maybe_periodic_reset();
        }
        assert!(s.has_stable());
    }

    #[test]
    fn format_change_clears_stable() {
        let mut s = stab();
        let r = raw(0, 10, 320, 230);
        for i in 0..3 {
            s.observe(r, Some(i * 40_000));
        }
        assert!(s.has_stable());
        s.set_geometry(640, 480, ChromaGrid::Yuv420);
        assert!(!s.has_stable());
    }

    #[test]
    fn three_40ms_inward_samples_do_not_shrink() {
        let mut s = stab();
        let full = raw(0, 0, 320, 240);
        let crop = raw(40, 40, 280, 200);
        for i in 0..3 {
            s.observe(full, Some(i * 40_000));
        }
        let before = s.current_aligned().unwrap().0;
        for i in 0..3 {
            s.observe(crop, Some(120_000 + i * 40_000));
        }
        let after = s.current_aligned().unwrap().0;
        assert_eq!(
            before, after,
            "three 40 ms inward samples (~100 ms) must not shrink"
        );
    }

    #[test]
    fn inward_hold_requires_200ms_span() {
        let mut s = stab();
        let full = raw(0, 0, 320, 240);
        let crop = raw(40, 40, 280, 200);
        for i in 0..3 {
            s.observe(full, Some(i * 10_000));
        }
        let before = s.current_aligned().unwrap().0;
        // 7 inward samples at 10 ms: 70% majority, span 60 ms < 200 ms.
        for i in 0..7 {
            s.observe(crop, Some(30_000 + i * 10_000));
        }
        let mid = s.current_aligned().unwrap().0;
        assert_eq!(
            before, mid,
            "70% inward support under a 200 ms hold must not shrink"
        );
        // First support at 30 ms; last at 230 ms → span 200 ms.
        for t in (100_000..=230_000).step_by(10_000) {
            s.observe(crop, Some(t));
        }
        let after = s.current_aligned().unwrap().0;
        assert!(
            after.x > before.x || after.y > before.y || after.w < before.w || after.h < before.h,
            "inward crop spanning 200 ms must be allowed, before={before:?} after={after:?}"
        );
    }
}
