package bf

type HashFunc func([]byte) uint32

type BloomFilter struct {
	hf     []HashFunc
	bitArr []byte
}

func NewBloomFilter(size int) *BloomFilter {

	return &BloomFilter{
		bitArr: make([]byte, size),
		hf: []HashFunc{
			RSHash,
			JSHash,
			PJWHash,
			ELFHash,
			BKDRHash,
			SDBMHash,
			DJBHash,
			DEKHash,
			BPHash,
			FNVHash,
		},
	}
}

func (bf *BloomFilter) Set(k []byte) {
	for _, hashFunc := range bf.hf {
		hashBit := hashFunc(k)
		//fmt.Println(hashBit)
		bf.set(hashBit)
	}
}

func (bf *BloomFilter) Get(k []byte) bool {
	ret := true
	for _, hashFunc := range bf.hf {
		hashBit := hashFunc(k)
		//fmt.Println(hashBit)
		ret = bf.get(hashBit) && ret
	}
	return ret
}

func (bf *BloomFilter) Exist(k []byte) bool {
	c := 0
	for _, hashFunc := range bf.hf {
		hashBit := hashFunc(k)
		//fmt.Println(hashBit)
		r := bf.get(hashBit)
		if r == false {
			return false
		} else {
			c++
		}
	}
	return c == len(bf.hf)
}

func (bf *BloomFilter) set(bit uint32) {
	idx := bit / 8
	offset := bit % 8
	bf.bitArr[idx] |= uint8(1 << offset)
}

func (bf *BloomFilter) get(bit uint32) bool {
	idx := bit / 8
	offset := bit % 8

	mask := byte(1 << offset)
	// 使用按位与操作来检查该位是否为 1
	return bf.bitArr[idx]&mask != 0
}
