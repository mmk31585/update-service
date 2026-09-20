package operations

import (
	"context"

	"update/internal/application/ports"
	"update/internal/domain/operation"
)

type ListOperationInput struct {
	Status    *operation.OperationStatus
	Processor *string
	Cursor    *string
	Limit     int
}

type ListOperationOutput struct {
	Operations []*operation.Operation
	NextCursor *string
}

type ListOperationUseCase struct {
	operations ports.OperationRepository
}

func NewListOperationUseCase(operations ports.OperationRepository) *ListOperationUseCase {
	return &ListOperationUseCase{
		operations: operations,
	}
}

func (uc *ListOperationUseCase) Execute(ctx context.Context, input ListOperationInput) (*ListOperationOutput, error) {
	filter := ports.OperationFilter{
		Status:    input.Status,
		Processor: input.Processor,
		Cursor:    input.Cursor,
		Limit:     input.Limit,
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	ops, err := uc.operations.List(ctx, filter)
	if err != nil {
		return nil, err
	}
	return &ListOperationOutput{
		Operations: ops,
		NextCursor: nil, // TODO: implement cursor pagination
	}, nil
}
