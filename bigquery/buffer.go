package bigquery

type uploadCompletionFunc func(u uploadBuffer, err error)

// Fields are public to make testing easier
type uploadBuffer struct {
	len  int
	buf  []byte
	Data [][]byte
	f    uploadCompletionFunc
}

func (u *uploadBuffer) regenerate() {
	u.len = 0
	u.buf = make([]byte, maxUploadBytes)
	u.Data = make([][]byte, 0, maxUploadCount)
}

func (u *uploadBuffer) zero() {
	u.len = 0
	u.buf = nil
	u.Data = nil
}

func (u *uploadBuffer) spaceFor(size int) bool {
	return u.len+size <= cap(u.buf) && len(u.Data) < maxUploadCount
}

func (u *uploadBuffer) next(size int) []byte {
	return u.buf[u.len : size+u.len]
}

func (u *uploadBuffer) add(buf []byte) {
	u.len += len(buf)
	u.Data = append(u.Data, buf)
}

func (u *uploadBuffer) combine(v uploadBuffer) (ok bool) {
	if len(u.buf) == 0 {
		*u = v
		return true
	}

	if u.len+v.len > cap(u.buf) {
		return false
	}
	if len(u.Data)+len(v.Data) > cap(u.Data) {
		return false
	}
	copy(u.buf[u.len:], v.buf[:v.len])
	for _, d := range v.Data {
		l := len(d)
		u.Data = append(u.Data, u.buf[u.len:u.len+l])
		u.len += l
	}
	return true
}
