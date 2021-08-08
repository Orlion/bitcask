package codec

import (
	"bufio"
	"encoding/binary"
	"github.com/Orlion/bitcask/internal"
	"github.com/pkg/errors"
	"io"
)

const (
	keySize      = 4
	valueSize    = 8
	checksumSize = 4
	ttlSize      = 8
	MetaInfoSize = keySize + valueSize + checksumSize + ttlSize
)

type Encoder struct {
	w *bufio.Writer
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w: bufio.NewWriter(w),
	}
}

func (e *Encoder) Encode(msg internal.Entry) (int64, error) {
	var bufKeyValue = make([]byte, keySize+valueSize)
	// 将key的长度存到buf中
	binary.BigEndian.PutUint32(bufKeyValue[:keySize], uint32(len(msg.Key)))
	// 将value的长度存到buf中
	binary.BigEndian.PutUint64(bufKeyValue[keySize:valueSize], uint64(len(msg.Value)))
	// 将key&value长度写入到文件
	if _, err := e.w.Write(bufKeyValue); err != nil {
		return 0, errors.Wrap(err, "failed writing key & value length prefix")
	}
	// 将key写入到文件
	if _, err := e.w.Write(msg.Key); err != nil {
		return 0, errors.Wrap(err, "failed writing key data")
	}
	// 将value写入到文件中
	if _, err := e.w.Write(msg.Value); err != nil {
		return 0, errors.Wrap(err, "failed writing value data")
	}
	// 将校验和存入文件中
	// 这里是复用了上面的kv buf, 不用再次申请内存
	bufChecksumSize := bufKeyValue[:checksumSize]
	binary.BigEndian.PutUint32(bufChecksumSize, msg.Checksum)
	if _, err := e.w.Write(bufChecksumSize); err != nil {
		return 0, errors.Wrap(err, "failed writing checksum data")
	}

	// 写入ttl
	// 同样的套路，依然是复用上面的 kv buf
	bufTTL := bufKeyValue[:ttlSize]
	if msg.Expiry == nil {
		binary.BigEndian.PutUint64(bufTTL, 0)
	} else {
		binary.BigEndian.PutUint64(bufTTL, uint64(msg.Expiry.Unix()))
	}
	if _, err := e.w.Write(bufTTL); err != nil {
		return 0, errors.Wrap(err, "failed writing ttl data")
	}

	// 将缓冲刷到文件系统，注意不是文件系统刷盘
	if err := e.w.Flush(); err != nil {
		return 0, errors.Wrap(err, "failed flushing data")
	}

	return int64(keySize + valueSize + len(msg.Key) + len(msg.Value) + checksumSize + ttlSize), nil
}
