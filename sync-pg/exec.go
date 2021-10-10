package pg

import (
	"context"
	"database/sql"
)

func ExecuteAll(ctx context.Context, db *sql.DB, stmts ...Statement) (int64, error) {
	if stmts == nil || len(stmts) == 0 {
		return 0, nil
	}
	tx, er1 := db.Begin()
	if er1 != nil {
		return 0, er1
	}
	var count int64
	count = 0
	for _, stmt := range stmts {
		r2, er3 := tx.ExecContext(ctx, stmt.Query, stmt.Params...)
		if er3 != nil {
			er4 := tx.Rollback()
			if er4 != nil {
				return count, er4
			}
			return count, er3
		}
		a2, er5 := r2.RowsAffected()
		if er5 != nil {
			tx.Rollback()
			return count, er5
		}
		count = count + a2
	}
	er6 := tx.Commit()
	return count, er6
}

