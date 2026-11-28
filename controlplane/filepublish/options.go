package filepublish

import (
	"time"

	"github.com/blackbirdworks/semaphore/controlplane"
)

// Defaults of the lock wait and of the body limit.
const (
	// DefaultPollInterval is the time between two tries of the lock.
	DefaultPollInterval = 50 * time.Millisecond
	// DefaultNoticeAfter is the wait after which the notice "waiting for the publish lock" is given once.
	DefaultNoticeAfter = 2 * time.Second
)

type settings struct {
	after       func(time.Duration) <-chan time.Time
	notice      func(string)
	poll        time.Duration
	noticeAfter time.Duration
	maxBody     int64
}

// Option changes how a [Store] waits for the lock and what it accepts.
type Option func(*settings)

// WithTimer replaces the timer of the lock wait (default [time.After]); a test drives the wait with it.
func WithTimer(after func(time.Duration) <-chan time.Time) Option {
	return func(s *settings) { s.after = after }
}

// WithNotice sets the function that receives the message "waiting for the publish lock <path>", once per wait, after
// the lock was busy for [DefaultNoticeAfter] (see [WithNoticeAfter]).
func WithNotice(notice func(string)) Option {
	return func(s *settings) { s.notice = notice }
}

// WithPollInterval sets the time between two tries of the lock. Zero or negative keeps the default.
func WithPollInterval(d time.Duration) Option {
	return func(s *settings) {
		if d > 0 {
			s.poll = d
		}
	}
}

// WithNoticeAfter sets how long the lock must stay busy before the notice. Zero or negative keeps the default.
func WithNoticeAfter(d time.Duration) Option {
	return func(s *settings) {
		if d > 0 {
			s.noticeAfter = d
		}
	}
}

// WithMaxBodyBytes bounds the body of one write; a larger one is [ErrTooLarge]. Zero or negative keeps the default,
// controlplane.DefaultMaxObjectBytes.
func WithMaxBodyBytes(limit int64) Option {
	return func(s *settings) {
		if limit > 0 {
			s.maxBody = limit
		}
	}
}

func newSettings(opts []Option) settings {
	set := settings{
		after: time.After, poll: DefaultPollInterval, noticeAfter: DefaultNoticeAfter,
		maxBody: controlplane.DefaultMaxObjectBytes,
	}

	for _, opt := range opts {
		opt(&set)
	}

	return set
}
