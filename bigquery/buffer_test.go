package bigquery

import (
	"bytes"
	"testing"
)

func TestBufferSpaceFor(t *testing.T) {
	tests := []struct {
		name string
		pre  []int
		size int
		exp  bool
	}{
		{
			name: "zero",
			pre:  nil,
			size: 0,
			exp:  true,
		},
		{
			name: "one",
			pre:  nil,
			size: 1,
			exp:  true,
		},
		{
			name: "maxUploadBytes",
			pre:  nil,
			size: maxUploadBytes,
			exp:  true,
		},
		{
			name: "maxUploadBytes+1",
			pre:  nil,
			size: maxUploadBytes + 1,
			exp:  false,
		},
		{
			name: "maxUploadCount",
			pre:  repeat(1, maxUploadCount),
			size: 1,
			exp:  false,
		},
		{
			name: "maxUploadCount-1",
			pre:  repeat(1, maxUploadCount-1),
			size: 1,
			exp:  true,
		},
		{
			name: "big pre",
			pre:  []int{maxUploadBytes},
			size: 1,
			exp:  false,
		},
		{
			name: "biggish pre",
			pre:  []int{maxUploadBytes - 1},
			size: 1,
			exp:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var b uploadBuffer
			b.regenerate()
			for i, s := range test.pre {
				if !b.spaceFor(s) {
					t.Fatalf("spaceFor(%d) returned false", s)
				}
				d := b.next(s)
				if len(d) != s {
					t.Fatalf("next(%d) returned %d", s, len(d))
				}

				// Adding some content so we can make sure it's preserved
				for j := range d {
					d[j] = byte(i)
				}

				b.add(d)
			}

			content := bytes.Join(b.Data, nil)
			if !bytes.Equal(content, b.buf[:b.len]) {
				t.Fatalf("content mismatch: %v != %v", content, b.buf[:b.len])
			}

			val := byte(0xFF)
			for _, d := range content {
				if d == val {
					continue
				}
				if d != val+1 {
					t.Fatalf("content mismatch: %d is not successor of %d", d, val)
				}
				val = d
			}

			if val != byte(len(test.pre)-1) {
				t.Fatalf("content mismatch: %d != %v", val, len(test.pre)-1)
			}

			if b.spaceFor(test.size) != test.exp {
				t.Fatalf("spaceFor(%d) returned %v", test.size, !test.exp)
			}
		})
	}
}

func repeat(val int, count int) []int {
	ret := make([]int, count)
	for i := range ret {
		ret[i] = val
	}
	return ret
}

func TestBufferCombine(t *testing.T) {
	tests := []struct {
		name       string
		a          []int
		b          []int
		exp        []int
		expCombine bool
	}{
		{
			name:       "empty",
			a:          nil,
			b:          nil,
			exp:        nil,
			expCombine: true,
		},
		{
			name:       "empty a",
			a:          nil,
			b:          []int{1},
			exp:        []int{1},
			expCombine: true,
		},
		{
			name:       "empty b",
			a:          []int{1},
			b:          nil,
			exp:        []int{1},
			expCombine: true,
		},
		{
			name:       "one",
			a:          []int{1},
			b:          []int{2},
			exp:        []int{1, 2},
			expCombine: true,
		},
		{
			name:       "two",
			a:          []int{1, 2},
			b:          []int{3, 4},
			exp:        []int{1, 2, 3, 4},
			expCombine: true,
		},
		{
			name:       "maxUploadBytes split",
			a:          []int{maxUploadBytes - 1},
			b:          []int{1},
			exp:        []int{maxUploadBytes - 1, 1},
			expCombine: true,
		},
		{
			name:       "maxUploadBytes+1 split",
			a:          []int{maxUploadBytes},
			b:          []int{1},
			expCombine: false,
			exp:        []int{maxUploadBytes},
		},
		{
			name:       "Max count split",
			a:          repeat(1, maxUploadCount-1),
			b:          []int{1},
			expCombine: true,
			exp:        repeat(1, maxUploadCount),
		},
		{
			name:       "Max count +1 split",
			a:          repeat(1, maxUploadCount-1),
			b:          []int{1, 1},
			expCombine: false,
			exp:        repeat(1, maxUploadCount-1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var a, b uploadBuffer
			if len(test.a) > 0 {
				a.regenerate()
			} else {
				a.zero()
			}
			b.regenerate()
			for _, s := range test.a {
				d := a.next(s)
				for i := range d {
					d[i] = byte(s)
				}
				a.add(d)
			}
			for _, s := range test.b {
				d := b.next(s)
				for i := range d {
					d[i] = byte(s)
				}
				b.add(d)
			}

			if act := a.combine(b); act != test.expCombine {
				t.Fatalf("combine mismatch: %v != %v", act, test.expCombine)
			}

			content := bytes.Join(a.Data, nil)
			if !bytes.Equal(content, a.buf[:a.len]) {
				t.Fatalf("content mismatch: %v != %v", content, a.buf[:a.len])
			}

			var offset int
			for _, d := range test.exp {
				for j := 0; j < d; j++ {
					if a.buf[offset+j] != byte(d) {
						t.Fatalf("content mismatch: %d != %d", a.buf[offset+j], d)
					}
				}
				offset += d
			}
			if offset != a.len {
				t.Fatalf("length mismatch: %d != %d", offset, a.len)
			}
		})
	}
}
