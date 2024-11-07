package dbaccess

import (
	"context"
	"fmt"
)

// WithTx starts and commits a transaction on a driver executor around the given function.
func WithTx(ctx context.Context, ds DataSource, innerFunc func(ctx context.Context, _ DataSource) error) error {
	_, err := WithTxV(ctx, ds, func(ctx context.Context, tds DataSource) (struct{}, error) {
		return struct{}{}, innerFunc(ctx, tds)
	})
	return err
}

// WithTxV starts and commits a transaction on a driver executor
// around the given function, allowing the return of a generic value.
func WithTxV[T any](ctx context.Context, ds DataSource, innerFunc func(ctx context.Context, _ DataSource) (T, error)) (T, error) {
	var defaultRes T

	tx, err := ds.Begin(ctx)
	if err != nil {
		return defaultRes, fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	res, err := innerFunc(ctx, tx)
	if err != nil {
		return defaultRes, err
	}

	if err := tx.Commit(ctx); err != nil {
		return defaultRes, fmt.Errorf("error committing transaction: %w", err)
	}

	return res, nil
}
