package coordinator

import "github.com/segmentio/kafka-go"

type MessagesHeap []kafka.Message

func (h MessagesHeap) Len() int           { return len(h) }
func (h MessagesHeap) Less(i, j int) bool { return h[i].Offset < h[j].Offset }
func (h MessagesHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MessagesHeap) Push(x interface{}) {
	*h = append(*h, x.(kafka.Message))
}

func (h *MessagesHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}


