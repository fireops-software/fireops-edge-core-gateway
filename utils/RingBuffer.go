package utils

type RingBuffer[T comparable] struct {
	data         []T
	currentIndex int
}

func (b *RingBuffer[T]) Push(item T) {
	b.data[b.currentIndex] = item
	b.currentIndex = (b.currentIndex + 1) % len(b.data)
}

func (b *RingBuffer[T]) Clear() {
	b.currentIndex = 0
	b.data = make([]T, len(b.data))
}

func (b *RingBuffer[T]) Contains(item T) bool {
	for _, element := range b.data {
		if element == item {
			return true
		}
	}
	return false
}

func NewRingBuffer[T comparable](size uint) *RingBuffer[T] {
	return &RingBuffer[T]{
		data:         make([]T, size),
		currentIndex: 0,
	}
}
