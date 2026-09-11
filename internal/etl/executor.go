package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"

	"github.com/JustinK33/Conduit/internal/retry"
	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const taskName = "sql.etl"

type Executor struct {
	pool *pgxpool.Pool
}

type PipelineSpec struct {
	ExtractSQL      string   `json:"extract_sql"`
	TargetTable     string   `json:"target_table"`
	TargetColumns   []string `json:"target_columns"`
	WriteMode       string   `json:"write_mode,omitempty"`
	ConflictColumns []string `json:"conflict_columns,omitempty"`
	UpdateColumns   []string `json:"update_columns,omitempty"`
}

func NewExecutor(pool *pgxpool.Pool) *Executor {
	return &Executor{pool: pool}
}

func TaskName() string {
	return taskName
}

func (e *Executor) Handler(ctx context.Context, job models.Job) error {
	spec, err := ParseSpec(job.Task.Payload)
	if err != nil {
		return fmt.Errorf("etl: invalid pipeline spec: %w: %w", err, retry.ErrNoRetry)
	}

	statement, err := BuildInsertSQL(spec)
	if err != nil {
		return fmt.Errorf("etl: invalid pipeline spec: %w: %w", err, retry.ErrNoRetry)
	}
	if e.pool == nil {
		return fmt.Errorf("etl: postgres pool is required")
	}

	tag, err := e.pool.Exec(ctx, statement)
	if err != nil {
		return fmt.Errorf("etl: execute pipeline: %w", err)
	}
	_ = tag.RowsAffected()
	return nil
}

func ParseSpec(payload []byte) (PipelineSpec, error) {
	if len(payload) == 0 {
		return PipelineSpec{}, fmt.Errorf("task.payload is required")
	}
	var spec PipelineSpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		return PipelineSpec{}, err
	}
	spec.ExtractSQL = strings.TrimSpace(spec.ExtractSQL)
	spec.TargetTable = strings.TrimSpace(spec.TargetTable)
	for i := range spec.TargetColumns {
		spec.TargetColumns[i] = strings.TrimSpace(spec.TargetColumns[i])
	}
	spec.WriteMode = strings.TrimSpace(strings.ToLower(spec.WriteMode))
	for i := range spec.ConflictColumns {
		spec.ConflictColumns[i] = strings.TrimSpace(spec.ConflictColumns[i])
	}
	for i := range spec.UpdateColumns {
		spec.UpdateColumns[i] = strings.TrimSpace(spec.UpdateColumns[i])
	}
	if spec.ExtractSQL == "" {
		return PipelineSpec{}, fmt.Errorf("extract_sql is required")
	}
	if spec.TargetTable == "" {
		return PipelineSpec{}, fmt.Errorf("target_table is required")
	}
	if len(spec.TargetColumns) == 0 {
		return PipelineSpec{}, fmt.Errorf("target_columns is required")
	}
	if spec.WriteMode == "" {
		spec.WriteMode = "append"
	}
	if spec.WriteMode != "append" && spec.WriteMode != "upsert" {
		return PipelineSpec{}, fmt.Errorf("write_mode must be append or upsert")
	}
	if spec.WriteMode == "upsert" && len(spec.ConflictColumns) == 0 {
		return PipelineSpec{}, fmt.Errorf("conflict_columns is required for upsert")
	}
	return spec, nil
}

func BuildInsertSQL(spec PipelineSpec) (string, error) {
	writeMode := strings.TrimSpace(strings.ToLower(spec.WriteMode))
	if writeMode == "" {
		writeMode = "append"
	}
	if writeMode != "append" && writeMode != "upsert" {
		return "", fmt.Errorf("write_mode must be append or upsert")
	}
	if writeMode == "upsert" && len(spec.ConflictColumns) == 0 {
		return "", fmt.Errorf("conflict_columns is required for upsert")
	}

	targetTable, err := parseIdentifier(spec.TargetTable)
	if err != nil {
		return "", fmt.Errorf("target_table: %w", err)
	}

	targetColumns := make([]string, 0, len(spec.TargetColumns))
	for _, column := range spec.TargetColumns {
		identifier, err := parseIdentifier(column)
		if err != nil {
			return "", fmt.Errorf("target_columns: %q: %w", column, err)
		}
		if len(identifier) != 1 {
			return "", fmt.Errorf("target column must be unqualified")
		}
		targetColumns = append(targetColumns, identifier.Sanitize())
	}

	conflictColumns, err := sanitizeUnqualifiedIdentifiers(spec.ConflictColumns)
	if err != nil {
		return "", fmt.Errorf("conflict_columns: %w", err)
	}
	updateColumns := spec.UpdateColumns
	if writeMode == "upsert" && len(updateColumns) == 0 {
		updateColumns = nonConflictColumns(spec.TargetColumns, spec.ConflictColumns)
	}
	sanitizedUpdateColumns, err := sanitizeUnqualifiedIdentifiers(updateColumns)
	if err != nil {
		return "", fmt.Errorf("update_columns: %w", err)
	}
	if writeMode == "upsert" && len(sanitizedUpdateColumns) == 0 {
		return "", fmt.Errorf("upsert requires at least one update column")
	}

	extractSQL, err := normalizeExtractSQL(spec.ExtractSQL)
	if err != nil {
		return "", fmt.Errorf("extract_sql: %w", err)
	}

	statement := fmt.Sprintf(
		"INSERT INTO %s (%s)\n%s",
		targetTable.Sanitize(),
		strings.Join(targetColumns, ", "),
		extractSQL,
	)
	if writeMode == "upsert" {
		assignments := make([]string, 0, len(sanitizedUpdateColumns))
		for _, column := range sanitizedUpdateColumns {
			assignments = append(assignments, fmt.Sprintf("%s = EXCLUDED.%s", column, column))
		}
		statement = fmt.Sprintf(
			"%s\nON CONFLICT (%s) DO UPDATE SET %s",
			statement,
			strings.Join(conflictColumns, ", "),
			strings.Join(assignments, ", "),
		)
	}
	return statement, nil
}

func sanitizeUnqualifiedIdentifiers(columns []string) ([]string, error) {
	sanitized := make([]string, 0, len(columns))
	for _, column := range columns {
		identifier, err := parseIdentifier(column)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", column, err)
		}
		if len(identifier) != 1 {
			return nil, fmt.Errorf("%q: column must be unqualified", column)
		}
		sanitized = append(sanitized, identifier.Sanitize())
	}
	return sanitized, nil
}

func nonConflictColumns(targetColumns []string, conflictColumns []string) []string {
	conflicts := make(map[string]struct{}, len(conflictColumns))
	for _, column := range conflictColumns {
		conflicts[column] = struct{}{}
	}
	var columns []string
	for _, column := range targetColumns {
		if _, ok := conflicts[column]; !ok {
			columns = append(columns, column)
		}
	}
	return columns
}

func normalizeExtractSQL(sql string) (string, error) {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimSuffix(sql, ";")
	sql = strings.TrimSpace(sql)
	lowerSQL := strings.ToLower(sql)
	if !strings.HasPrefix(lowerSQL, "select ") && !strings.HasPrefix(lowerSQL, "with ") {
		return "", fmt.Errorf("must start with SELECT or WITH")
	}

	tokens := sqlTokens(sql)
	if len(tokens) == 0 {
		return "", fmt.Errorf("empty SQL")
	}
	if tokens[0] != "select" && tokens[0] != "with" {
		return "", fmt.Errorf("must start with SELECT or WITH")
	}
	for _, token := range tokens {
		if forbiddenSQLToken(token) {
			return "", fmt.Errorf("forbidden token %q", token)
		}
	}
	return sql, nil
}

func parseIdentifier(value string) (pgx.Identifier, error) {
	parts := strings.Split(value, ".")
	if len(parts) == 0 || len(parts) > 3 {
		return nil, fmt.Errorf("must have one to three parts")
	}
	identifier := make(pgx.Identifier, 0, len(parts))
	for _, part := range parts {
		if !validIdentifierPart(part) {
			return nil, fmt.Errorf("invalid identifier")
		}
		identifier = append(identifier, part)
	}
	return identifier, nil
}

func validIdentifierPart(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if i == 0 {
			if r != '_' && !unicode.IsLetter(r) {
				return false
			}
			continue
		}
		if r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func forbiddenSQLToken(token string) bool {
	switch token {
	case "insert", "update", "delete", "merge", "create", "alter", "drop",
		"truncate", "copy", "grant", "revoke", "call", "do", "vacuum", "analyze":
		return true
	default:
		return false
	}
}

func sqlTokens(sql string) []string {
	var tokens []string
	var b strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	inLineComment := false
	inBlockComment := false

	flush := func() {
		if b.Len() == 0 {
			return
		}
		tokens = append(tokens, strings.ToLower(b.String()))
		b.Reset()
	}

	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		next := byte(0)
		if i+1 < len(sql) {
			next = sql[i+1]
		}

		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && next == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if inSingleQuote {
			if ch == '\'' {
				if next == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		}
		if inDoubleQuote {
			if ch == '"' {
				if next == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		if ch == '-' && next == '-' {
			flush()
			inLineComment = true
			i++
			continue
		}
		if ch == '/' && next == '*' {
			flush()
			inBlockComment = true
			i++
			continue
		}
		if ch == '\'' {
			flush()
			inSingleQuote = true
			continue
		}
		if ch == '"' {
			flush()
			inDoubleQuote = true
			continue
		}
		if unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' {
			b.WriteByte(ch)
			continue
		}
		flush()
	}
	flush()
	return tokens
}
