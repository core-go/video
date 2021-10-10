package cassandra

import (
	"context"
	"encoding/hex"
	"github.com/gocql/gocql"
)

func Query(ses *gocql.Session, fieldsIndex map[string]int, results interface{}, sql string, values ...interface{}) error {
	q := ses.Query(sql, values...)
	if q.Exec() != nil {
		return q.Exec()
	}
	return ScanIter(q.Iter(), results, fieldsIndex)
}
func QueryWithPage(ses *gocql.Session, fieldsIndex map[string]int, results interface{}, sql string, values []interface{}, max int, options ...string) (string, error) {
	nextPageToken := ""
	if len(options) > 0 && len(options[0]) > 0 {
		nextPageToken = options[0]
	}
	next, er0 := hex.DecodeString(nextPageToken)
	if er0 != nil {
		return "", er0
	}
	query := ses.Query(sql, values...).PageState(next).PageSize(max)
	if query.Exec() != nil {
		return "", query.Exec()
	}
	err := ScanIter(query.Iter(), results, fieldsIndex)
	if err != nil {
		return "", err
	}
	nextPageToken = hex.EncodeToString(query.Iter().PageState())
	return nextPageToken, nil
}
func Exec(ses *gocql.Session, query string, values...interface{}) (int64, error) {
	q := ses.Query(query, values...)
	err := q.Exec()
	if err != nil {
		return 0, err
	}
	return 1, nil
}
func ExecuteAll(ctx context.Context, ses *gocql.Session, stmts ...Statement) (int64, error) {
	return ExecuteAllWithSize(ctx, ses, 5, stmts...)
}
func ExecuteAllWithSize(ctx context.Context, ses *gocql.Session, size int, stmts ...Statement) (int64, error) {
	if stmts == nil || len(stmts) == 0 {
		return 0, nil
	}
	batch := ses.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	l := len(stmts)
	for i := 0; i < l; i++ {
		var args []interface{}
		args = stmts[i].Params
		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt:       stmts[i].Query,
			Args:       args,
			Idempotent: true,
		})
		if i % size == 0 || i == l - 1 {
			err := ses.ExecuteBatch(batch)
			if err != nil {
				return int64(i + 1) , err
			}
			batch = ses.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		}
	}
	return int64(l), nil
}
