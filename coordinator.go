package coordinator

import (
	"container/heap"
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

type ProcessorFunc func(m kafka.Message)

type metrics struct {
	consumed  prometheus.Counter
	committed prometheus.Counter
	queueSize prometheus.Gauge
	running   prometheus.Gauge
}

func newMetrics(namespace string) metrics {
	m := metrics{
		consumed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "coordinator",
			Name:      "consumed_tasks",
			Help:      "number of consumed tasks",
		}),
		committed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "coordinator",
			Name:      "committed_tasks",
			Help:      "number of committed tasks",
		}),
		queueSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "coordinator",
			Name:      "queue_size",
			Help:      "number of queued tasks",
		}),
		running: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "coordinator",
			Name:      "running_tasks",
			Help:      "number of running tasks",
		}),
	}
	prometheus.MustRegister(m.consumed, m.committed, m.queueSize, m.running)
	return m
}

type Coordinator struct {
	ctx           context.Context
	metrics       metrics
	reader        *kafka.Reader
	commitQueue   chan kafka.Message
	processor     ProcessorFunc
}

func (c *Coordinator) readNext() kafka.Message {
	msg, err := c.reader.FetchMessage(c.ctx)
	logrus.Infof("Fetched message: %d", msg.Offset)
	if err != nil {
		logrus.Fatalf("Failed to read next message: %s", err.Error())
	}
	return msg
}

func (c *Coordinator) processingLoop() {
	for {
		msg := c.readNext()
		c.metrics.consumed.Inc()
		c.metrics.running.Inc()
		c.processor(msg)
		c.metrics.running.Dec()
		c.commitQueue <- msg
	}
}

func offsets(h []kafka.Message) string {
	res := new(strings.Builder)
	for i, v := range h {
		if i != 0 {
			res.WriteByte(',')
		}
		res.WriteString(strconv.Itoa(int(v.Offset)))
	}
	return res.String()
}

func (c *Coordinator) commitLoop(expectedOffset int64, logger *logrus.Logger) {
	logger.Infof("Starting coordinator with initial offset: %d", expectedOffset)
	h := new(MessagesHeap)
	targets := make([]kafka.Message, 0)
	for processed := range c.commitQueue {
		heap.Push(h, processed)
		c.metrics.queueSize.Set(float64(h.Len()))
		// extract longest increasing subsequence starting from expected offset
		for {
			if h.Len() != 0 && (*h)[0].Offset == expectedOffset {
				expectedOffset++
				targets = append(targets, heap.Pop(h).(kafka.Message))
			} else {
				break
			}
		}
		if len(targets) > 0 {
			logger.Infof("Preparing to commit offsets `%s`", offsets(targets))
			if err := c.reader.CommitMessages(c.ctx, targets...); err != nil {
				logger.Fatalf("Failed to commit messages: %s", err.Error())
			}
			c.metrics.committed.Add(float64(len(targets)))
			logger.Info("Offsets committed. Requesting next task")
			targets = targets[:0]
		}
		logger.Infof("Waiting for offset `%d`", expectedOffset)
	}
}

func (c *Coordinator) Commit(msg kafka.Message) error {
	return c.reader.CommitMessages(c.ctx, msg)
}

func (c *Coordinator) Stop() (err error) {
	return c.reader.Close()
}

func New(ctx context.Context, reader *kafka.Reader, workersCount int, processor ProcessorFunc) *Coordinator {
	c := &Coordinator{
		ctx:         ctx,
		processor:   processor,
		metrics:     newMetrics(reader.Config().Topic),
		commitQueue: make(chan kafka.Message, workersCount),
		reader:      reader}

	first := c.readNext()
	go c.commitLoop(first.Offset, logrus.StandardLogger())
	c.commitQueue <- first

	for i := 0; i < workersCount; i++ {
		go c.processingLoop()
	}

	return c
}
