package bigquery

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryQ(t *testing.T) {
	tests := []struct {
		name       string
		tw         chan uploadBuffer
		dw         chan uploadBuffer
		msgCount   int
		msgSize    int
		expTWCount int
		expDWCount int
		wait       time.Duration
	}{
		{
			name: "nothing",
			tw:   make(chan uploadBuffer),
			dw:   make(chan uploadBuffer, 10),
		},
		{
			name:       "tw not receiving, combine",
			tw:         make(chan uploadBuffer),
			dw:         make(chan uploadBuffer, 10),
			msgSize:    1,
			msgCount:   10,
			expTWCount: 0,
			expDWCount: 1,
			wait:       time.Second * 30,
		},
		{
			name:       "tw not receiving, can't combine",
			tw:         make(chan uploadBuffer),
			dw:         make(chan uploadBuffer, 10),
			msgSize:    maxUploadBytes,
			msgCount:   10,
			expTWCount: 0,
			expDWCount: 10,
			wait:       time.Second * 30,
		},

		{
			name:       "tw receiving, can't combine",
			tw:         make(chan uploadBuffer, 10),
			dw:         make(chan uploadBuffer, 10),
			msgSize:    maxUploadBytes,
			msgCount:   10,
			expTWCount: 10,
			expDWCount: 0,
		},
		{
			name:       "tw not receiving, can't combine, little wait",
			tw:         make(chan uploadBuffer),
			dw:         make(chan uploadBuffer, 10),
			msgSize:    maxUploadBytes,
			msgCount:   10,
			expTWCount: 0,
			expDWCount: 10,
		},
	}

	aerr := errors.New("aerr")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rq := newRetryQueue(tt.tw, tt.dw)
			rq.wait = time.Microsecond
			if tt.wait != 0 {
				rq.wait = tt.wait
			}
			rq.start(ctx)

			for i := 0; i < tt.msgCount; i++ {
				var u uploadBuffer
				u.regenerate()
				u.add(u.next(tt.msgSize))
				rq.completionFunc(u, aerr)
			}

			time.Sleep(time.Millisecond)
			rq.stop()

			if len(tt.tw) != tt.expTWCount {
				t.Errorf("tw count: got %d, want %d", len(tt.tw), tt.expTWCount)
			}
			if len(tt.dw) != tt.expDWCount {
				t.Errorf("dw count: got %d, want %d", len(tt.dw), tt.expDWCount)
			}
		})
	}
}
