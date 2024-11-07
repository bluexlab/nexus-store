package dbaccess

import (
	"context"

	"github.com/jackc/pgx/v5"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
)

type DataSource interface {
	dbsqlc.DBTX
	Begin(ctx context.Context) (pgx.Tx, error)
}
