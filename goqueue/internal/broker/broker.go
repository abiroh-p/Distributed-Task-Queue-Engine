package broker

import (
    "github.com/redis/go-redis/v9"
	"context"
	"fmt"
    "time"
    "strings"
)
const (
    StreamHigh    = "goqueue:stream:high"
    StreamDefault = "goqueue:stream:default"
    StreamLow     = "goqueue:stream:low"
    GroupName     = "goqueue-workers"
)

const DLQStream = "goqueue:stream:dlq"
type Message struct {
    StreamID string
    Stream   string
    JobID    string
    Payload  string
}

type Broker struct {
    rdb *redis.Client
}

func New(addr string) *Broker {
    rdb := redis.NewClient(&redis.Options{
        Addr:        addr,
        ReadTimeout: 10 * time.Second,
    })
    return &Broker{rdb: rdb}
}

func (b *Broker) Enqueue(ctx context.Context, priority int, jobID string, payload string) error {
    stream := priorityToStream(priority)

    return b.rdb.XAdd(ctx, &redis.XAddArgs{
        Stream: stream,
        Values: map[string]interface{}{
            "job_id":  jobID,
            "payload": payload,
        },
    }).Err()
}

func priorityToStream(priority int) string {
    switch {
    case priority >= 8:
        return StreamHigh
    case priority >= 4:
        return StreamDefault
    default:
        return StreamLow
    }
}

func (b *Broker) CreateConsumerGroup(ctx context.Context) error {
    streams := []string{StreamHigh, StreamDefault, StreamLow}

    for _, stream := range streams {
        err := b.rdb.XGroupCreateMkStream(ctx, stream, GroupName, "0").Err()
        if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
            return fmt.Errorf("creating group for stream %s: %w", stream, err)
        }
    }
    return nil
}

func (b *Broker) Consume(ctx context.Context, consumerName string) (*Message, error) {
    streams := []string{StreamHigh, StreamDefault, StreamLow}
    streamArgs := make([]string, len(streams)*2)

    for i, s := range streams {
        streamArgs[i] = s
        streamArgs[len(streams)+i] = ">"
    }

    results, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
    Group:    GroupName,
    Consumer: consumerName,
    Streams:  streamArgs,
    Count:    1,
    Block:    3000,
    }).Result()

   if err != nil {
    if err == redis.Nil {
        return nil, nil
    }
    // ignore i/o timeout — normal when no jobs arrive during block window
    if strings.Contains(err.Error(), "i/o timeout") {
        return nil, nil
    }
    return nil, fmt.Errorf("xreadgroup: %w", err)
}
    for _, result := range results {
        for _, msg := range result.Messages {
            return &Message{
                StreamID: msg.ID,
                Stream:   result.Stream,
                JobID:    fmt.Sprintf("%v", msg.Values["job_id"]),
                Payload:  fmt.Sprintf("%v", msg.Values["payload"]),
            }, nil
        }
    }

    return nil, nil
}

func (b *Broker) Ack(ctx context.Context, stream, streamID string) error {
    return b.rdb.XAck(ctx, stream, GroupName, streamID).Err()
}

func (b *Broker) AddToDLQ(ctx context.Context, jobID, queue, payload, reason string) error {
    return b.rdb.XAdd(ctx, &redis.XAddArgs{
        Stream: DLQStream,
        Values: map[string]interface{}{
            "job_id":  jobID,
            "queue":   queue,
            "payload": payload,
            "reason":  reason,
        },
    }).Err()
}

