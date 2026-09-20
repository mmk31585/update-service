package operations

import (
	"context"
	"errors"

	"update/internal/application/ports"
	"update/internal/domain/operation"
	"update/internal/domain/result"
)

type GetResultOperationInput struct {
	OperationID operation.OperationID
}

type GetResultOperationOutput struct {
	Result *result.Result
}

var ErrResultNotReady = errors.New("result not ready - operation not COMPLETED")
var ErrResultExpired = errors.New("result expired - removed by retention policy")

type GetResultOperationUseCase struct {
	operations ports.OperationRepository
	results    ports.ResultRepository
}

func NewGetResultOperationUseCase(
	operations ports.OperationRepository,
	results ports.ResultRepository,
) *GetResultOperationUseCase {
	return &GetResultOperationUseCase{
		operations: operations,
		results:    results,
	}
}

func (uc *GetResultOperationUseCase) Execute(ctx context.Context, input GetResultOperationInput) (*GetResultOperationOutput, error) {
	op, err := uc.operations.GetByID(ctx, input.OperationID)
	if err != nil {
		return nil, err
	}
	if op == nil {
		return nil, ErrOperationNotFound
	}

	if op.Status != operation.StatusCompleted {
		return nil, ErrResultNotReady
	}

	res, err := uc.results.GetByOperationID(ctx, op.ID.String())
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, ErrResultNotReady
	}

	// TODO: Check if result is expired based on retention policy

	return &GetResultOperationOutput{
		Result: res,
	}, nil
}
