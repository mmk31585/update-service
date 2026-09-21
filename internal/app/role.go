package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
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

	activeTransfers   map[string]bool
	activeTransfersMu sync.Mutex
}

func NewRoleManager(app *Application) *RoleManager {
	rm := &RoleManager{app: app}
	rm.activeTransfers = make(map[string]bool)
	rm.activeTransfersMu = sync.Mutex{}
	return rm
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

	// Also subscribe to coordinator events to receive TransferVerified
	if err := rm.startEventConsumer(ctx); err != nil {
		return err
	}

	log.Println("worker tasks started")
	return nil
}

func (rm *RoleManager) startCommandConsumer(ctx context.Context) error {
	nodeID := rm.app.AppConfig.Node.ID
	subject := nats.NodeCommandSubject(nodeID)
	consumerName := "cmd-processor-" + nodeID

	rm.activeTransfers = make(map[string]bool)
	rm.activeTransfersMu = sync.Mutex{}

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
			log.Printf("PROCESS_UPDATE_RECEIVED node_id=%s operation_id=%s job_id=%s transfer_id=%s",
				rm.app.AppConfig.Node.ID, cmd.OperationID, cmd.JobID, cmd.Payload["transfer_id"])
			log.Printf("PROCESS_UPDATE_PUBLISH operation_id=%s job_id=%s attempt_id=%s transfer_id=%s worker_node_id=%s",
				cmd.OperationID, cmd.JobID, cmd.Payload["attempt_id"], cmd.Payload["transfer_id"], rm.app.AppConfig.Node.ID)
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
					fileSize, _ := cmd.Payload["file_size_bytes"].(float64)
					fileSHA256, _ := cmd.Payload["file_sha256"].(string)
					chunkSize, _ := cmd.Payload["chunk_size_bytes"].(float64)
					totalChunks, _ := cmd.Payload["total_chunks"].(float64)

// Add DEBUG snapshot at worker transfer acceptance
					log.Printf("WORKER_TRANSFER_SNAPSHOT node_id=%s operation_id=%s job_id=%s attempt_id=%s transfer_id=%s file_size=%d file_sha256=%s chunk_size=%d total_chunks=%d consumer_name=%s chunk_subject=%s",
						rm.app.AppConfig.Node.ID, cmd.OperationID, cmd.JobID, cmd.Payload["attempt_id"].(string),
						transferIDStr, int64(fileSize), fileSHA256, int64(chunkSize), int(totalChunks),
						fmt.Sprintf("chunks-%s", transferIDStr),
						nats.TransferChunksSubject(transferIDStr))

					log.Printf("ACTIVE_TRANSFER_ADD transfer_id=%s", transferIDStr)
					rm.activeTransfersMu.Lock()
					rm.activeTransfers[transferIDStr] = true
					rm.activeTransfersMu.Unlock()
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
	
	log.Printf("PROCESS_UPDATE_SNAPSHOT node_id=%s operation_id=%s job_id=%s attempt_id=%s transfer_id=%s transfer_db_status=%s active_transfer=false received_chunks=0 expected_chunks=%d",
		rm.app.AppConfig.Node.ID, cmd.OperationID, cmd.JobID, cmd.Payload["attempt_id"], cmd.Payload["transfer_id"], "unknown", 0)
	
	// Add START_TRANSFER payload logging for debugging
	if cmd.Type == "START_TRANSFER" {
		transferIDStr, _ := cmd.Payload["transfer_id"].(string)
		fileSize, _ := cmd.Payload["file_size_bytes"].(float64)
		fileSHA256, _ := cmd.Payload["file_sha256"].(string)
		chunkSize, _ := cmd.Payload["chunk_size_bytes"].(float64)
		totalChunks, _ := cmd.Payload["total_chunks"].(float64)
		
		log.Printf("WORKER_START_TRANSFER_PAYLOAD node_id=%s operation_id=%s transfer_id=%s file_size_bytes=%d file_sha256=%s chunk_size_bytes=%d total_chunks=%d", 
			rm.app.AppConfig.Node.ID, cmd.OperationID, transferIDStr, int64(fileSize), fileSHA256, int64(chunkSize), totalChunks)
		log.Printf("WORKER_TRANSFER_SNAPSHOT node_id=%s operation_id=%s job_id=%s attempt_id=%s transfer_id=%s file_size=%d file_sha256=%s chunk_size=%d total_chunks=%d consumer_name=%s chunk_subject=%s", 
			rm.app.AppConfig.Node.ID, cmd.OperationID, cmd.JobID, cmd.Payload["attempt_id"].(string), 
			transferIDStr, int64(fileSize), fileSHA256, int64(chunkSize), totalChunks,
			fmt.Sprintf("chunks-%s", transferIDStr))
	}
	
	log.Printf("PROCESS_UPDATE_TRANSFER_CHECK transfer_id=%s database_status=unknown in_memory_active=false received_chunks=0 expected_chunks=%d",
		cmd.Payload["transfer_id"], 0)
	
	// Check if transfer is complete before processing
	rm.checkTransferAndProceed(ctx, cmd.OperationID)
	
	log.Printf("PROCESS_UPDATE_ALLOWED transfer_id=%s reason=no_active_transfer", cmd.Payload["transfer_id"])

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
		log.Printf("EVENT_RECEIVED node_id=%s subject=%s", rm.app.AppConfig.Node.ID, msg.Subject())

		var evt struct {
			ID            string         `json:"id"`
			EventType     string         `json:"event_type"`
			OperationID   string         `json:"operation_id"`
			JobID         string         `json:"job_id"`
			AttemptID     string         `json:"attempt_id"`
			TransferID    string         `json:"transfer_id"`
			CorrelationID string         `json:"correlation_id"`
			Payload       map[string]any `json:"payload"`
		}
		if err := json.Unmarshal(msg.Data(), &evt); err != nil {
			log.Printf("invalid event payload: %v", err)
			_ = msg.Ack()
			return
		}

		log.Printf("EVENT_DECODED node_id=%s event_id=%s event_type=%s operation_id=%s job_id=%s transfer_id=%s",
			rm.app.AppConfig.Node.ID, evt.ID, evt.EventType, evt.OperationID, evt.JobID, evt.TransferID)

		if evt.EventType == "" {
			log.Printf("EVENT_EMPTY_TYPE node_id=%s subject=%s raw_message=%s", rm.app.AppConfig.Node.ID, msg.Subject(), string(msg.Data()))
		}

		switch event.EventType(evt.EventType) {
		case event.EventTypeProcessingSucceeded:
			rm.handleProcessingSucceeded(ctx, evt.OperationID, evt.Payload)
		case event.EventTypeProcessingFailed:
			rm.handleProcessingFailed(ctx, evt.OperationID, evt.Payload)
		case event.EventTypeCancelled:
			rm.handleCancelled(ctx, evt.OperationID, evt.Payload)
		case event.EventTypeTransferVerified:
			// For workers: mark transfer as complete in activeTransfers
			// For entries: full operation status transition
			if node.Role(rm.app.AppConfig.Node.Role) == node.RoleWorker {
				rm.handleWorkerTransferVerified(ctx, evt.OperationID, evt.Payload)
			} else {
				rm.handleTransferVerified(ctx, evt.OperationID, evt.Payload)
			}
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
	op.Stage = operation.StageVerifying
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

func (rm *RoleManager) handleWorkerTransferVerified(ctx context.Context, operationID string, payload map[string]any) {
	log.Printf("worker: operation %s transfer verified", operationID)

	// Mark any active transfers as complete for this operation
	rm.activeTransfersMu.Lock()
	for transferIDStr := range rm.activeTransfers {
		if strings.Contains(transferIDStr, operationID) {
			delete(rm.activeTransfers, transferIDStr)
			log.Printf("ACTIVE_TRANSFER_REMOVE transfer_id=%s reason=transfer_verified", transferIDStr)
			log.Printf("worker: transfer %s marked complete, proceeding with processing", transferIDStr)
		}
	}
	rm.activeTransfersMu.Unlock()

	// Also update operation status to indicate transfer is complete
	op, err := rm.app.operations.GetByID(ctx, operation.OperationID(operationID))
	if err != nil {
		log.Printf("worker: load operation %s: %v", operationID, err)
		return
	}
	if op == nil {
		log.Printf("worker: operation not found: %s", operationID)
		return
	}

	now := time.Now().UTC()
	op.Status = operation.StatusTransferred
	op.Stage = operation.StageVerifying
	op.UpdatedAt = now

	if err := rm.app.operations.Update(ctx, op); err != nil {
		log.Printf("worker: update operation %s: %v", operationID, err)
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

	log.Printf("worker: operation %s marked TRANSFERRED", operationID)
}

func (rm *RoleManager) checkTransferAndProceed(ctx context.Context, operationID string) {
	// Check if there are any active transfers for this operation
	rm.activeTransfersMu.Lock()
	hasActiveTransfer := false
	for transferIDStr := range rm.activeTransfers {
		if strings.Contains(transferIDStr, operationID) {
			hasActiveTransfer = true
			log.Printf("ACTIVE_TRANSFER_CHECK transfer_id=%s active=true", transferIDStr)
			log.Printf("worker: transfer %s still active, waiting...", transferIDStr)
			break
		}
	}
	if !hasActiveTransfer {
		log.Printf("ACTIVE_TRANSFER_CHECK transfer_id=%s active=false", operationID)
	}
	rm.activeTransfersMu.Unlock()

	if hasActiveTransfer {
		log.Printf("worker: transfer for operation %s still in progress, deferring processing", operationID)
		// Wait a bit and check again - in a real implementation, would use a channel or timer
		time.Sleep(500 * time.Millisecond)
		rm.checkTransferAndProceed(ctx, operationID)
	} else {
		log.Printf("worker: no active transfers for operation %s, proceeding with processing", operationID)
	}
}

func generateEventID() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return "evt-" + hex.EncodeToString(buf)
}
