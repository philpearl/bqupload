package bigquery

import (
	"testing"
)

func TestPump(t *testing.T) {
	tests := []struct {
		name       string
		tw         chan uploadBuffer
		dw         chan uploadBuffer
		msgCount   int
		msgSize    int
		expTWCount int
		expDWCount int
	}{
		{
			name:       "nothing",
			tw:         make(chan uploadBuffer),
			dw:         make(chan uploadBuffer, 10),
			msgSize:    maxUploadBytes,
			msgCount:   0,
			expTWCount: 0,
			expDWCount: 0,
		},

		{
			name:       "tw not receiving",
			tw:         make(chan uploadBuffer),
			dw:         make(chan uploadBuffer, 10),
			msgSize:    maxUploadBytes,
			msgCount:   10,
			expTWCount: 0,
			expDWCount: 10,
		},
		{
			name:       "tw receiving",
			tw:         make(chan uploadBuffer, 1),
			dw:         make(chan uploadBuffer, 10),
			msgSize:    maxUploadBytes,
			msgCount:   10,
			expTWCount: 1,
			expDWCount: 9,
		},

		{
			name:       "several small, tw receiving",
			tw:         make(chan uploadBuffer, 1),
			dw:         make(chan uploadBuffer, 10),
			msgSize:    1,
			msgCount:   10,
			expTWCount: 1,
			expDWCount: 0,
		},

		{
			name:       "several small, tw not receiving",
			tw:         make(chan uploadBuffer),
			dw:         make(chan uploadBuffer, 10),
			msgSize:    1,
			msgCount:   10,
			expTWCount: 0,
			expDWCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := newPump(test.tw, test.dw, nil)

			for i := 0; i < test.msgCount; i++ {
				buf := p.BufferFor(test.msgSize)
				p.Add(buf)
			}
			p.Flush()

			if len(test.tw) != test.expTWCount {
				t.Errorf("tw count: got %d, want %d", len(test.tw), test.expTWCount)
			}
			if len(test.dw) != test.expDWCount {
				t.Errorf("dw count: got %d, want %d", len(test.dw), test.expDWCount)
			}
		})
	}
}
