package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"sync"
	"time"

	"update/internal/domain/event"
	"update/internal/domain/node"
	"update/internal/domain/operation"

	"update/internal/infrastructure/nats"

	"github.com/nats-io/nats.go/jetstream"
)

type RoleManager struct {
	app *Application
}

func NewRoleManager(app *Application) *RoleManager {
	return &RoleManager{app: app}
}

func (rm *RoleManager) Start(ctx context.Context) error {
	role := node.ParseRole(rm.app.AppConfig.Node.Role)
	log.Printf("starting node role: %s", role)

	if err := rm.registerNode(ctx); err != nil {
		return err
	}

	rm.startHeartbeat(ctx)

	if role.IsWorker() {
		if err := rm.startWorkerTasks(ctx); err != nil {
			return err
		}
	}

	if role.IsEntry() {
		if err := rm.startCoordinatorTasks(ctx); err != nil {
			return err
		}
	}

	log.Printf("node %s started as %s (incarnation=%s)",
		rm.app.AppConfig.Node.ID, role, rm.app.AppConfig.Node.IncarnationID)
	return nil
}

func (rm *RoleManager) registerNode(ctx context.Context) error {
	now := time.Now().UTC()
	role := node.ParseRole(rm.app.AppConfig.Node.Role)
	roles := []string{role.String()}
	if role.IsEntry() {
		roles = append(roles, "entry")
	}
	if role.IsWorker() {
		roles = append(roles, "worker")
	}

	n := &node.Node{
		ID:                   rm.app.AppConfig.Node.ID,
		IncarnationID:        rm.app.AppConfig.Node.IncarnationID,
		IncarnationStartedAt: now,
		Status:               node.NodeStatusStarting,
		Roles:                roles,
		HeartbeatSequence:    0,
		RegisteredAt:         now,
		UpdatedAt:            now,
	}

	if err := rm.app.nodes.Upsert(ctx, n); err != nil {
		return err
	}

	log.Printf("node registered: id=%s incarnation=%s", n.ID, n.IncarnationID)
	return nil
}

func (rm *RoleManager) startHeartbeat(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(rm.app.AppConfig.Node.HeartbeatInterval)
		defer ticker.Stop()

		seq := uint64(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				seq++
				now := time.Now().UTC()
				deadlineAt := now.Add(rm.app.AppConfig.Node.HeartbeatTimeout)

				role := node.ParseRole(rm.app.AppConfig.Node.Role)
				hb := &node.Heartbeat{
					NodeID:              rm.app.AppConfig.Node.ID,
					IncarnationID:       rm.app.AppConfig.Node.IncarnationID,
					Sequence:            seq,
					Status:              node.NodeStatusHealthy,
					Capabilities:        []string{role.String()},
					ActiveJobs:          0,
					ActiveTransfers:     0,
					ObservedAt:          now,
					HeartbeatDeadlineAt: deadlineAt,
				}

				if err := rm.app.heartbeats.Insert(ctx, hb); err != nil {
					log.Printf("insert heartbeat: %v", err)
					continue
				}

				if err := rm.app.nodes.UpdateHeartbeat(ctx, rm.app.AppConfig.Node.ID, seq, deadlineAt); err != nil {
					log.Printf("update heartbeat: %v", err)
				}

				if err := rm.app.nodes.UpdateStatus(ctx, rm.app.AppConfig.Node.ID, node.NodeStatusHealthy); err != nil {
					log.Printf("update status: %v", err)
				}
			}
		}
	}()
}

type ProcessCommand struct {
	CommandID   string         `json:"command_id"`
	OperationID string         `json:"operation_id"`
	JobID       string         `json:"job_id"`
	Type        string         `json:"type"`
	Payload     map[string]any `json:"payload"`
}

func (rm *RoleManager) startWorkerTasks(ctx context.Context) error {
	if rm.app.nats == nil {
		log.Println("nats client not configured, skipping worker tasks")
		return nil
	}

	if err := rm.startCommandConsumer(ctx); err != nil {
		return err
	}

	log.Println("worker tasks started")
	return nil
}

func (rm *RoleManager) startCommandConsumer(ctx context.Context) error {
	nodeID := rm.app.AppConfig.Node.ID
	subject := nats.NodeCommandSubject(nodeID)
	consumerName := "cmd-processor-" + nodeID

	var activeTransfers map[string]bool
	activeTransfers = make(map[string]bool)
	var activeTransfersMu sync.Mutex

	_, err := rm.app.nats.Subscribe(ctx, subject, consumerName, func(msg jetstream.Msg) {
		log.Printf("worker %s received command on %s", nodeID, msg.Subject())

		var cmd ProcessCommand
		if err := json.Unmarshal(msg.Data(), &cmd); err != nil {
			log.Printf("invalid command payload: %v", err)
			_ = msg.Ack()
			return
		}

		switch cmd.Type {
		case "PROCESS_UPDATE":
			rm.handleProcessJob(ctx, cmd)
			_ = msg.Ack()
		case "CANCEL_UPDATE":
			rm.handleCancelJob(ctx, cmd)
			_ = msg.Ack()
		case "START_TRANSFER":
			if rm.app.transferReceiver != nil {
				_, err := rm.app.transferReceiver.AcceptTransfer(ctx, cmd.Payload)
				if err != nil {
					log.Printf("worker %s: accept transfer failed: %v", nodeID, err)
					_ = msg.Nak()
				} else {
					transferIDStr, _ := cmd.Payload["transfer_id"].(string)
					activeTransfersMu.Lock()
					activeTransfers[transferIDStr] = true
					activeTransfersMu.Unlock()
					_ = msg.Ack()
				}
			} else {
				_ = msg.Nak()
			}
		default:
			log.Printf("unknown command type: %s", cmd.Type)
			_ = msg.Ack()
		}
	})
	if err != nil {
		return err
	}

	log.Printf("command consumer started: %s (consumer=%s)", subject, consumerName)
	return nil
}

func (rm *RoleManager) handleProcessJob(ctx context.Context, cmd ProcessCommand) {
	log.Printf("worker %s processing operation %s (job=%s)", rm.app.AppConfig.Node.ID, cmd.OperationID, cmd.JobID)

	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		log.Printf("worker %s: context cancelled during processing of %s", rm.app.AppConfig.Node.ID, cmd.OperationID)
		return
	}

	now := time.Now().UTC()
	evt := &event.OperationEvent{
		ID:            event.EventID(generateEventID()),
		OperationID:   cmd.OperationID,
		EventType:     event.EventTypeProcessingSucceeded,
		Payload:       map[string]any{"command_id": cmd.CommandID, "job_id": cmd.JobID, "result": map[string]any{"status": "ok"}},
		CorrelationID: cmd.OperationID,
		IssuedAt:      now,
		ActorNodeID:   rm.app.AppConfig.Node.ID,
		Version:       1,
	}

	if err := rm.app.events.Append(ctx, evt); err != nil {
		log.Printf("worker %s: failed to append success event for %s: %v", rm.app.AppConfig.Node.ID, cmd.OperationID, err)
		return
	}

	eventPayload, _ := json.Marshal(evt)
	if err := rm.app.nats.PublishEvent(ctx, string(evt.EventType), eventPayload); err != nil {
		log.Printf("worker %s: failed to publish event for %s: %v", rm.app.AppConfig.Node.ID, cmd.OperationID, err)
	}

	log.Printf("worker %s completed processing for operation %s", rm.app.AppConfig.Node.ID, cmd.OperationID)
}

func (rm *RoleManager) handleCancelJob(ctx context.Context, cmd ProcessCommand) {
	log.Printf("worker %s cancelling operation %s (job=%s)", rm.app.AppConfig.Node.ID, cmd.OperationID, cmd.JobID)

	now := time.Now().UTC()
	evt := &event.OperationEvent{
		ID:            event.EventID(generateEventID()),
		OperationID:   cmd.OperationID,
		EventType:     event.EventTypeCancelled,
		Payload:       map[string]any{"command_id": cmd.CommandID, "job_id": cmd.JobID},
		CorrelationID: cmd.OperationID,
		IssuedAt:      now,
		ActorNodeID:   rm.app.AppConfig.Node.ID,
		Version:       1,
	}

	_ = rm.app.events.Append(ctx, evt)
	eventPayload, _ := json.Marshal(evt)
	_ = rm.app.nats.PublishEvent(ctx, string(evt.EventType), eventPayload)
}

func (rm *RoleManager) startCoordinatorTasks(ctx context.Context) error {
	if rm.app.nats == nil {
		log.Println("nats client not configured, skipping coordinator tasks")
		return nil
	}

	if err := rm.startEventConsumer(ctx); err != nil {
		return err
	}

	log.Println("coordinator tasks started")
	return nil
}

func (rm *RoleManager) startEventConsumer(ctx context.Context) error {
	consumerName := "coordinator-events-" + rm.app.AppConfig.Node.ID
	_, err := rm.app.nats.Subscribe(ctx, nats.SubjectCoordinatorEvents, consumerName, func(msg jetstream.Msg) {
		log.Printf("coordinator received event on %s", msg.Subject())

		var evt struct {
			EventType   string         `json:"event_type"`
			OperationID string         `json:"operation_id"`
			Payload     map[string]any `json:"payload"`
		}
		if err := json.Unmarshal(msg.Data(), &evt); err != nil {
			log.Printf("invalid event payload: %v", err)
			_ = msg.Ack()
			return
		}

		switch event.EventType(evt.EventType) {
		case event.EventTypeProcessingSucceeded:
			rm.handleProcessingSucceeded(ctx, evt.OperationID, evt.Payload)
		case event.EventTypeProcessingFailed:
			rm.handleProcessingFailed(ctx, evt.OperationID, evt.Payload)
		case event.EventTypeCancelled:
			rm.handleCancelled(ctx, evt.OperationID, evt.Payload)
		case event.EventTypeTransferVerified:
			rm.handleTransferVerified(ctx, evt.OperationID, evt.Payload)
		default:
			log.Printf("unhandled event type: %s", evt.EventType)
		}

		_ = msg.Ack()
	})
	if err != nil {
		return err
	}

	log.Printf("event consumer started: %s (consumer=%s)", nats.SubjectCoordinatorEvents, consumerName)
	return nil
}

func (rm *RoleManager) handleProcessingSucceeded(ctx context.Context, operationID string, payload map[string]any) {
	log.Printf("coordinator: operation %s processing succeeded", operationID)

	op, err := rm.app.operations.GetByID(ctx, operation.OperationID(operationID))
	if err != nil {
		log.Printf("coordinator: load operation %s: %v", operationID, err)
		return
	}
	if op == nil {
		log.Printf("coordinator: operation not found: %s", operationID)
		return
	}

	now := time.Now().UTC()
	op.Status = operation.StatusCompleted
	op.Stage = operation.StageCompleted
	op.UpdatedAt = now

	if err := rm.app.operations.Update(ctx, op); err != nil {
		log.Printf("coordinator: update operation %s: %v", operationID, err)
		return
	}

	evt := &event.OperationEvent{
		ID:            event.EventID(generateEventID()),
		OperationID:   operationID,
		EventType:     event.EventTypeOperationCompleted,
		Payload:       map[string]any{"payload": payload},
		CorrelationID: operationID,
		IssuedAt:      now,
		ActorNodeID:   rm.app.AppConfig.Node.ID,
		Version:       1,
	}
	_ = rm.app.events.Append(ctx, evt)

	if rm.app.elasticIndexer != nil {
		rm.app.elasticIndexer.Index(ctx, op, evt)
	}

	log.Printf("coordinator: operation %s marked COMPLETED", operationID)
}

func (rm *RoleManager) handleProcessingFailed(ctx context.Context, operationID string, payload map[string]any) {
	log.Printf("coordinator: operation %s processing failed", operationID)

	op, err := rm.app.operations.GetByID(ctx, operation.OperationID(operationID))
	if err != nil {
		log.Printf("coordinator: load operation %s: %v", operationID, err)
		return
	}
	if op == nil {
		log.Printf("coordinator: operation not found: %s", operationID)
		return
	}

	now := time.Now().UTC()
	op.Status = operation.StatusFailed
	op.Stage = operation.StageFailed
	op.UpdatedAt = now
	op.Error = &operation.OperationError{
		Code:    "PROCESSING_FAILED",
		Message: "Worker reported processing failure",
		Details: payload,
	}

	if err := rm.app.operations.Update(ctx, op); err != nil {
		log.Printf("coordinator: update operation %s: %v", operationID, err)
		return
	}

	evt := &event.OperationEvent{
		ID:            event.EventID(generateEventID()),
		OperationID:   operationID,
		EventType:     event.EventTypeOperationFailed,
		Payload:       map[string]any{"payload": payload},
		CorrelationID: operationID,
		IssuedAt:      now,
		ActorNodeID:   rm.app.AppConfig.Node.ID,
		Version:       1,
	}
	_ = rm.app.events.Append(ctx, evt)

	if rm.app.elasticIndexer != nil {
		rm.app.elasticIndexer.Index(ctx, op, evt)
	}

	log.Printf("coordinator: operation %s marked FAILED", operationID)
}

func (rm *RoleManager) handleCancelled(ctx context.Context, operationID string, payload map[string]any) {
	log.Printf("coordinator: operation %s cancelled", operationID)

	op, err := rm.app.operations.GetByID(ctx, operation.OperationID(operationID))
	if err != nil {
		log.Printf("coordinator: load operation %s: %v", operationID, err)
		return
	}
	if op == nil {
		log.Printf("coordinator: operation not found: %s", operationID)
		return
	}

	now := time.Now().UTC()
	op.Status = operation.StatusCancelled
	op.Stage = operation.StageCancelled
	op.UpdatedAt = now

	if err := rm.app.operations.Update(ctx, op); err != nil {
		log.Printf("coordinator: update operation %s: %v", operationID, err)
		return
	}

	evt := &event.OperationEvent{
		ID:            event.EventID(generateEventID()),
		OperationID:   operationID,
		EventType:     event.EventTypeOperationCancelled,
		Payload:       map[string]any{"payload": payload},
		CorrelationID: operationID,
		IssuedAt:      now,
		ActorNodeID:   rm.app.AppConfig.Node.ID,
		Version:       1,
	}
	_ = rm.app.events.Append(ctx, evt)

	if rm.app.elasticIndexer != nil {
		rm.app.elasticIndexer.Index(ctx, op, evt)
	}

	log.Printf("coordinator: operation %s marked CANCELLED", operationID)
}

func (rm *RoleManager) handleTransferVerified(ctx context.Context, operationID string, payload map[string]any) {
	log.Printf("coordinator: operation %s transfer verified", operationID)

	op, err := rm.app.operations.GetByID(ctx, operation.OperationID(operationID))
	if err != nil {
		log.Printf("coordinator: load operation %s: %v", operationID, err)
		return
	}
	if op == nil {
		log.Printf("coordinator: operation not found: %s", operationID)
		return
	}

	now := time.Now().UTC()
	op.Status = operation.StatusTransferred
	op.Stage = operation.StageTransferring
	op.UpdatedAt = now

	if err := rm.app.operations.Update(ctx, op); err != nil {
		log.Printf("coordinator: update operation %s: %v", operationID, err)
		return
	}

	evt := &event.OperationEvent{
		ID:            event.EventID(generateEventID()),
		OperationID:   operationID,
		EventType:     event.EventTypeOperationTransferred,
		Payload:       map[string]any{"payload": payload},
		CorrelationID: operationID,
		IssuedAt:      now,
		ActorNodeID:   rm.app.AppConfig.Node.ID,
		Version:       1,
	}
	_ = rm.app.events.Append(ctx, evt)

	log.Printf("coordinator: operation %s marked TRANSFERRED", operationID)
}

func generateEventID() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return "evt-" + hex.EncodeToString(buf)
}
