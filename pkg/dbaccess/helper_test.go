package dbaccess

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockDataSource is a mock implementation of the DataSource interface
type MockDataSource struct {
	mock.Mock
}

func (m *MockDataSource) Begin(ctx context.Context) (pgx.Tx, error) {
	args := m.Called(ctx)
	return args.Get(0).(pgx.Tx), args.Error(1)
}

// Implement dbsqlc.DBTX methods
func (m *MockDataSource) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	args := m.Called(ctx, sql, arguments)
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

func (m *MockDataSource) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	callArgs := m.Called(ctx, sql, args)
	return callArgs.Get(0).(pgx.Rows), callArgs.Error(1)
}

func (m *MockDataSource) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	callArgs := m.Called(ctx, sql, args)
	return callArgs.Get(0).(pgx.Row)
}

// MockTx is a mock implementation of the pgx.Tx interface
type MockTx struct {
	mock.Mock
}

func (m *MockTx) Begin(ctx context.Context) (pgx.Tx, error) {
	args := m.Called(ctx)
	return args.Get(0).(pgx.Tx), args.Error(1)
}

func (m *MockTx) Commit(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockTx) Rollback(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	args := m.Called(ctx, sql, arguments)
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

func (m *MockTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	callArgs := m.Called(ctx, sql, args)
	return callArgs.Get(0).(pgx.Rows), callArgs.Error(1)
}

func (m *MockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	callArgs := m.Called(ctx, sql, args)
	return callArgs.Get(0).(pgx.Row)
}

func (m *MockTx) Conn() *pgx.Conn {
	args := m.Called()
	return args.Get(0).(*pgx.Conn)
}

func (m *MockTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	args := m.Called(ctx, tableName, columnNames, rowSrc)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	args := m.Called(ctx, b)
	return args.Get(0).(pgx.BatchResults)
}

func (m *MockTx) LargeObjects() pgx.LargeObjects {
	args := m.Called()
	return args.Get(0).(pgx.LargeObjects)
}

func (m *MockTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	args := m.Called(ctx, name, sql)
	return args.Get(0).(*pgconn.StatementDescription), args.Error(1)
}

func (m *MockTx) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Implement other methods of pgx.Tx interface as needed...

func TestWithTx(t *testing.T) {
	ctx := context.Background()

	t.Run("successful transaction", func(t *testing.T) {
		mockDS := new(MockDataSource)
		mockTx := new(MockTx)

		mockDS.On("Begin", ctx).Return(mockTx, nil)
		mockTx.On("Commit", ctx).Return(nil)
		mockTx.On("Rollback", ctx).Return(nil)

		err := WithTx(ctx, mockDS, func(ctx context.Context, ds DataSource) error {
			// We can't directly compare ds to mockTx because ds is of type DataSource
			// Instead, we can check if it's not nil
			assert.NotNil(t, ds)
			return nil
		})

		assert.NoError(t, err)
		mockDS.AssertExpectations(t)
		mockTx.AssertExpectations(t)
	})

	t.Run("begin transaction error", func(t *testing.T) {
		mockDS := new(MockDataSource)
		mockDS.On("Begin", ctx).Return((*MockTx)(nil), errors.New("begin error"))

		err := WithTx(ctx, mockDS, func(ctx context.Context, ds DataSource) error {
			t.Fatal("This function should not be called")
			return nil
		})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error beginning transaction")
		mockDS.AssertExpectations(t)
	})

	t.Run("inner function error", func(t *testing.T) {
		mockDS := new(MockDataSource)
		mockTx := new(MockTx)

		mockDS.On("Begin", ctx).Return(mockTx, nil)
		mockTx.On("Rollback", ctx).Return(nil)

		err := WithTx(ctx, mockDS, func(ctx context.Context, ds DataSource) error {
			return errors.New("inner error")
		})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "inner error")
		mockDS.AssertExpectations(t)
		mockTx.AssertExpectations(t)
	})

	t.Run("commit error", func(t *testing.T) {
		mockDS := new(MockDataSource)
		mockTx := new(MockTx)

		mockDS.On("Begin", ctx).Return(mockTx, nil)
		mockTx.On("Commit", ctx).Return(errors.New("commit error"))
		mockTx.On("Rollback", ctx).Return(nil)

		err := WithTx(ctx, mockDS, func(ctx context.Context, ds DataSource) error {
			return nil
		})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error committing transaction")
		mockDS.AssertExpectations(t)
		mockTx.AssertExpectations(t)
	})
}
