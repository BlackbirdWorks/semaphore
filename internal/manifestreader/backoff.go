package manifestreader

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
)

// backoffFactor is the growth of the backoff ceiling per consecutive failure.
const backoffFactor = 2

// maxBackoffExp bounds the exponent, so the ceiling cannot overflow before the cap applies.
const maxBackoffExp = 30

// BackoffStatus is the backoff state at the end of a round.
type BackoffStatus struct {
	// Wait is the wait planned before the next round. It is encoded as a number of nanoseconds.
	Wait time.Duration `json:"wait"`
	// Failures is the number of consecutive failed manifest polls (an error, a timeout, a 404, a manifest above
	// the byte limit); a 200 or a 304 resets it. A manifest that was read and rejected is not a failure of the poll.
	Failures int `json:"failures"`
	// ObjectsWaiting is the number of failed objects that are waiting for their next retry.
	ObjectsWaiting int `json:"objectsWaiting"`
	// Throttled tells that the last failure was a throttle: it backs off one step further.
	Throttled bool `json:"throttled"`
}

// PollCount is the number of polls with one result.
type PollCount struct {
	Result string `json:"result"`
	Count  uint64 `json:"count"`
}

// RejectionCount is the number of manifest rejections for one closed reason.
type RejectionCount struct {
	Reason string `json:"reason"`
	Count  uint64 `json:"count"`
}

// retryState is the schedule of one failed object: the number of failures in a row and the virtual time of the
// next try.
type retryState struct {
	at    time.Duration
	fails int
}

// sleep waits d on the scheduler and advances the virtual time when the wait completed.
func (r *Reader) sleep(ctx context.Context, delay time.Duration) error {
	err := r.sched.Wait(ctx, delay)
	if err != nil {
		return fmt.Errorf("manifestreader: %w", err)
	}

	r.vnow += delay

	return nil
}

// ceiling is min(BackoffMax, PollInterval x 2^exp): the top of the full jitter range.
func (r *Reader) ceiling(exp int) time.Duration {
	exp = min(max(exp, 0), maxBackoffExp)
	limit := float64(r.cfg.BackoffMax)

	return time.Duration(min(float64(r.cfg.PollInterval)*math.Pow(backoffFactor, float64(exp)), limit))
}

func isThrottled(err error) bool {
	fetchErr, ok := errors.AsType[*FetchError](err)

	return ok && fetchErr.Throttled
}

// planWait counts the poll and plans the wait before the next round. A successful poll (200 or 304) resets the
// backoff and the wait is the regular, jittered interval. After n failures in a row the wait is a full jitter
// draw in [0, ceiling(n)) with ceiling(n) = min(BackoffMax, PollInterval x 2^n), n + 1 for a throttle, and never
// shorter than the regular wait: a failure never makes the reader poll faster.
func (r *Reader) planWait(poll PollStatus, throttled bool) {
	r.pollCounts[poll.Result]++

	base := r.next()

	switch poll.Result {
	case PollError, PollNotFound, PollTooLarge:
		r.pollFails++

		exp := r.pollFails
		if throttled {
			exp++
		}

		full := time.Duration(float64(r.ceiling(exp)) * r.jitterValue())
		r.wait = max(base, full)
		r.backoff = BackoffStatus{Wait: r.wait, Failures: r.pollFails, Throttled: throttled}
	default:
		r.pollFails = 0
		r.wait = base
		r.backoff = BackoffStatus{Wait: base}
	}
}

// noteFailure schedules the next try of a failed object. A retryable failure waits a full jitter draw in
// [0, min(BackoffMax, PollInterval x 2^(n-1))) after n failures in a row; one that is not retryable (a denied
// request) waits BackoffMax each time, a slow fixed cadence: a retry cannot succeed without a change on the
// publisher or the configuration side, and a new manifest generation tries it at once anyway.
func (r *Reader) noteFailure(sum string, res result) {
	state := r.retry[sum]
	if state == nil {
		state = &retryState{}
		r.retry[sum] = state
	}

	state.fails++

	delay := r.cfg.BackoffMax
	if res.retryable {
		delay = time.Duration(float64(r.ceiling(state.fails-1)) * r.jitterValue())
	}

	state.at = r.vnow + delay
}

// dueGroups returns the groups to fetch in this round: the ones never tried and the ones whose retry time came.
func (r *Reader) dueGroups() []*group {
	var due []*group

	for _, grp := range r.groups {
		state := r.retry[grp.sum]
		if state == nil || state.at <= r.vnow {
			due = append(due, grp)
		}
	}

	return due
}

// objectsWaiting is the number of failed groups that are not due yet.
func (r *Reader) objectsWaiting() int {
	waiting := 0

	for _, grp := range r.groups {
		if state := r.retry[grp.sum]; state != nil && state.at > r.vnow {
			waiting++
		}
	}

	return waiting
}

// spreadWait waits the random delay before the fetches of a new generation: jitter x min(FetchSpread,
// PollInterval), unless the round was asked for by a refresh, which also ends the delay. It reports false when the
// context ended.
func (r *Reader) spreadWait(ctx context.Context) bool {
	if r.cfg.DisableFetchSpread || r.cfg.FetchSpread <= 0 || r.requested {
		return true
	}

	delay := time.Duration(r.jitterValue() * float64(r.cfg.FetchSpread))
	if delay <= 0 {
		return true
	}

	// a refresh that arrives during the delay cuts it short: the caller waits for the generation
	_, err := r.sleepOrRefresh(ctx, delay)

	return err == nil
}

// pollResults is the sorted list of the poll counts.
func (r *Reader) pollResults() []PollCount {
	if len(r.pollCounts) == 0 {
		return nil
	}

	out := make([]PollCount, 0, len(r.pollCounts))
	for result, count := range r.pollCounts {
		out = append(out, PollCount{Result: result, Count: count})
	}

	slices.SortFunc(out, func(a, b PollCount) int {
		switch {
		case a.Result < b.Result:
			return -1
		case a.Result > b.Result:
			return 1
		}

		return 0
	})

	return out
}

// rejectionResults is the sorted list of manifest rejection counts by reason.
func (r *Reader) rejectionResults() []RejectionCount {
	if len(r.rejectCounts) == 0 {
		return nil
	}

	out := make([]RejectionCount, 0, len(r.rejectCounts))
	for reason, count := range r.rejectCounts {
		out = append(out, RejectionCount{Reason: reason, Count: count})
	}

	slices.SortFunc(out, func(a, b RejectionCount) int {
		return cmp.Compare(a.Reason, b.Reason)
	})

	return out
}
