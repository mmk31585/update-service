package scheduling

import (
	"context"

	"update/internal/application/ports"
)

type Scheduler struct {
	nodes ports.NodeRepository
}

func NewScheduler(nodes ports.NodeRepository) *Scheduler {
	return &Scheduler{nodes: nodes}
}

type ScheduleResult struct {
	NodeID string
	Reason string
}

func (s *Scheduler) SelectWorker(ctx context.Context) (*ScheduleResult, error) {
	candidates, err := s.nodes.ListHealthy(ctx)
	if err != nil {
		return nil, err
	}

	for _, n := range candidates {
		for _, r := range n.Roles {
			if r == "worker" || r == "hybrid" {
				return &ScheduleResult{
					NodeID: n.ID,
					Reason: "first_healthy_worker",
				}, nil
			}
		}
	}

	return nil, ErrNoHealthyWorker
}

var ErrNoHealthyWorker = &ScheduleError{Message: "no healthy worker available"}

type ScheduleError struct {
	Message string
}

func (e *ScheduleError) Error() string {
	return e.Message
}
