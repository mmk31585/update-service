package nats

import "fmt"

const (
	SubjectNodeCommands      = "control.node.%s.commands"
	SubjectCoordinatorEvents = "control.events"
	SubjectNodeEvents        = "control.nodes.%s.events"
	SubjectTransferChunks    = "data.transfer.%s.chunks"
)

func NodeCommandSubject(nodeID string) string {
	return fmt.Sprintf(SubjectNodeCommands, nodeID)
}

func NodeEventsSubject(nodeID string) string {
	return fmt.Sprintf(SubjectNodeEvents, nodeID)
}

func TransferChunksSubject(transferID string) string {
	return fmt.Sprintf(SubjectTransferChunks, transferID)
}
