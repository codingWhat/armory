package redis

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_encodeMetadata(t *testing.T) {
	mt := &metadata{
		dataType: byte(String),
		expire:   time.Now().Unix(),
		version:  time.Now().Add(10 * time.Second).Unix(),
		size:     11,
	}

	bytes := encodeMetadata(mt)
	meta := decodeMetaData(bytes)

	assert.Equal(t, meta.dataType, mt.dataType)
	assert.Equal(t, meta.expire, mt.expire)
	assert.Equal(t, meta.version, mt.version)
	assert.Equal(t, meta.size, mt.size)
}

func Test_encodeMetadata_List(t *testing.T) {
	mt := &metadata{
		dataType: byte(List),
		expire:   time.Now().Unix(),
		version:  time.Now().Add(10 * time.Second).Unix(),
		size:     11,
		head:     123,
		tail:     2313,
	}

	bytes := encodeMetadata(mt)
	meta := decodeMetaData(bytes)

	assert.Equal(t, meta.dataType, mt.dataType)
	assert.Equal(t, meta.expire, mt.expire)
	assert.Equal(t, meta.version, mt.version)
	assert.Equal(t, meta.size, mt.size)
	assert.Equal(t, meta.head, mt.head)
	assert.Equal(t, meta.tail, mt.tail)
}
