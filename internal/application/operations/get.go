package operations

import (
	"context"

	"update/internal/application/ports"
	"update/internal/domain/operation"
)

type GetOperationUseCase struct {
	operations ports.OperationRepository
	baseURL    string
}

func NewGetOperationUseCase(operations ports.OperationRepository, baseURL string) *GetOperationUseCase {
	return &GetOperationUseCase{
		operations: operations,
		baseURL:    baseURL,
	}
}

func (uc *GetOperationUseCase) Execute(ctx context.Context, id operation.OperationID) (*operation.Operation, error) {
	return uc.operations.GetByID(ctx, id)
}
