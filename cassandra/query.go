package cassandra

import (
	"encoding/hex"
	"github.com/gocql/gocql"
)

func Exec(ses *gocql.Session, query string, values...interface{}) (int64, error) {
	q := ses.Query(query, values...)
	err := q.Exec()
	if err != nil {
		return 0, err
	}
	return 1, nil
}
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
