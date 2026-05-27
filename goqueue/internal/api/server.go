package api
import (
    "context"
    "time"
    "github.com/google/uuid"
    "github.com/rs/zerolog/log"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"

    "github.com/abishekP101/goqueue/internal/broker"
    "github.com/abishekP101/goqueue/internal/store"
    pb "github.com/abishekP101/goqueue/proto/goqueue/v1"
)

type Server struct {
    pb.UnimplementedJobServiceServer
    broker *broker.Broker
    store  *store.Store
}

func New(b *broker.Broker, s *store.Store) *Server {
    return &Server{
        broker: b,
        store:  s,
    }
}


func clampPriority(p int) int {
    if p < 1 {
        return 1
    }
    if p > 10 {
        return 10
    }
    return p
}

func (s *Server) EnqueueJob(ctx context.Context, req *pb.EnqueueJobRequest) (*pb.EnqueueJobResponse, error) {
    if req.Job == nil {
        return nil, status.Error(codes.InvalidArgument, "job is required")
    }

    job := store.Job{
        ID:         uuid.New().String(),
        Queue:      req.Job.Queue,
        Payload:    req.Job.Payload,
        Priority:   clampPriority(int(req.Job.Priority)),
        MaxRetries: int(req.Job.MaxRetries),
        RunAt:      time.Now(),
    }

    if job.Queue == "" {
        return nil, status.Error(codes.InvalidArgument, "queue is required")
    }
    if job.Payload == "" {
        return nil, status.Error(codes.InvalidArgument, "payload is required")
    }

    if err := s.store.CreateJob(ctx, job); err != nil {
        return nil, status.Errorf(codes.Internal, "failed to persist job: %v", err)
    }

    if err := s.broker.Enqueue(ctx, job.Priority, job.ID, job.Payload); err != nil {
        return nil, status.Errorf(codes.Internal, "failed to enqueue job: %v", err)
    }

    log.Info().Str("job_id", job.ID).Str("queue", job.Queue).Msg("job enqueued via gRPC")

    return &pb.EnqueueJobResponse{JobId: job.ID}, nil
}

func (s *Server) GetJobStatus(ctx context.Context, req *pb.GetJobStatusRequest) (*pb.GetJobStatusResponse, error) {
    if req.JobId == "" {
        return nil, status.Error(codes.InvalidArgument, "job_id is required")
    }

    job, err := s.store.GetJobByID(ctx, req.JobId)
    if err != nil {
        return nil, status.Errorf(codes.NotFound, "job not found: %v", err)
    }

    return &pb.GetJobStatusResponse{
        Job: &pb.Job{
            Id:         job.ID,
            Queue:      job.Queue,
            Payload:    job.Payload,
            Priority:   int32(job.Priority),
            MaxRetries: int32(job.MaxRetries),
            Status:     job.Status,
        },
    }, nil
}

func (s *Server) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
    if req.JobId == "" {
        return nil, status.Error(codes.InvalidArgument, "job_id is required")
    }

    job, err := s.store.GetJobByID(ctx, req.JobId)
    if err != nil {
        return nil, status.Errorf(codes.NotFound, "job not found: %v", err)
    }

    if job.Status == "running" {
        return nil, status.Error(codes.FailedPrecondition, "cannot cancel a running job")
    }
    if job.Status == "succeeded" || job.Status == "dead" {
        return nil, status.Errorf(codes.FailedPrecondition, "job already in terminal state: %s", job.Status)
    }

    if err := s.store.UpdateStatus(ctx, req.JobId, "cancelled"); err != nil {
        return nil, status.Errorf(codes.Internal, "failed to cancel job: %v", err)
    }

    log.Info().Str("job_id", req.JobId).Msg("job cancelled")
    return &pb.CancelJobResponse{Cancelled: true}, nil
}