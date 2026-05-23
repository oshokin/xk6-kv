package kv

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
)

// importCSVOptions holds parsed options for the corresponding KV method.
type importCSVOptions struct {
	// FileName is the path of the CSV or JSONL file.
	FileName string
	// KeyColumn names the CSV column used as the store key.
	KeyColumn string
	// Delimiter is the CSV field separator rune.
	Delimiter rune
	// HasHeader indicates whether the CSV file includes a header row.
	HasHeader bool
	// Limit caps how many rows or entries are processed.
	Limit int64
	// BatchSize is the number of rows written per store batch.
	BatchSize int64
}

// importImportCSVOptions parses Sobek options for the corresponding KV method.
func importImportCSVOptions(rt *sobek.Runtime, options sobek.Value) (importCSVOptions, error) {
	parsed := importCSVOptions{
		Delimiter: ',',
		HasHeader: true,
	}

	if common.IsNullish(options) {
		return parsed, NewError(
			InvalidOptionsError,
			"importCSV options must be an object with non-empty fileName and keyColumn",
		)
	}

	if err := ensureOptionalObjectOptions("importCSV", options); err != nil {
		return parsed, err
	}

	optionsObj := options.ToObject(rt)
	if err := parseImportCSVRequiredFields(&parsed, optionsObj); err != nil {
		return parsed, err
	}

	if err := parseImportCSVOptionalDelimiter(&parsed, optionsObj); err != nil {
		return parsed, err
	}

	if err := parseImportCSVOptionalHasHeader(&parsed, optionsObj); err != nil {
		return parsed, err
	}

	if err := validateImportCSVKeyColumn(parsed); err != nil {
		return parsed, err
	}

	if err := parseImportCSVOptionalLimit(&parsed, optionsObj); err != nil {
		return parsed, err
	}

	if err := parseImportCSVOptionalBatchSize(&parsed, optionsObj); err != nil {
		return parsed, err
	}

	return parsed, nil
}

// parseImportCSVRequiredFields parses and validates a single options field.
func parseImportCSVRequiredFields(parsed *importCSVOptions, optionsObj *sobek.Object) error {
	fileName, fileNameSet, err := parseOptionalStringOption("importCSV", "fileName", optionsObj.Get("fileName"))
	if err != nil {
		return err
	}

	if !fileNameSet || strings.TrimSpace(fileName) == "" {
		return NewError(
			InvalidOptionsError,
			"importCSV fileName is required",
		)
	}

	parsed.FileName = fileName

	keyColumn, keyColumnSet, err := parseOptionalStringOption("importCSV", "keyColumn", optionsObj.Get("keyColumn"))
	if err != nil {
		return err
	}

	if !keyColumnSet || strings.TrimSpace(keyColumn) == "" {
		return NewError(
			InvalidOptionsError,
			"importCSV keyColumn is required",
		)
	}

	parsed.KeyColumn = keyColumn

	return nil
}

// parseImportCSVOptionalDelimiter parses and validates a single options field.
func parseImportCSVOptionalDelimiter(parsed *importCSVOptions, optionsObj *sobek.Object) error {
	delimiter, err := parseOptionalCSVDelimiterOrDefault("importCSV", optionsObj, parsed.Delimiter)
	if err != nil {
		return err
	}

	parsed.Delimiter = delimiter

	return nil
}

// parseOptionalCSVDelimiterOrDefault parses and validates a single options field.
func parseOptionalCSVDelimiterOrDefault(method string, optionsObj *sobek.Object, fallback rune) (rune, error) {
	delimiter, delimiterSet, err := parseOptionalCSVDelimiter(method, optionsObj)
	if err != nil {
		return 0, err
	}

	if !delimiterSet {
		return fallback, nil
	}

	return delimiter, nil
}

// parseOptionalCSVDelimiter parses and validates a single options field.
func parseOptionalCSVDelimiter(method string, optionsObj *sobek.Object) (rune, bool, error) {
	delimiter, delimiterSet, err := parseOptionalStringOption(method, "delimiter", optionsObj.Get("delimiter"))
	if err != nil {
		return 0, false, err
	}

	if !delimiterSet {
		return 0, false, nil
	}

	r, err := parseCSVDelimiter(method, delimiter)
	if err != nil {
		return 0, false, err
	}

	return r, true, nil
}

// parseCSVDelimiter parses and validates a single options field.
func parseCSVDelimiter(method, delimiter string) (rune, error) {
	if delimiter == "" {
		return 0, NewError(
			InvalidOptionsError,
			method+" options.delimiter must be exactly one character",
		)
	}

	r, size := utf8.DecodeRuneInString(delimiter)
	if size == 0 || size != len(delimiter) {
		return 0, NewError(
			InvalidOptionsError,
			method+" options.delimiter must be exactly one character",
		)
	}

	if err := validateCSVDelimiter(method, r); err != nil {
		return 0, err
	}

	return r, nil
}

// validateCSVDelimiter validates user-supplied input.
func validateCSVDelimiter(method string, r rune) error {
	switch r {
	case '\r', '\n', '"', utf8.RuneError:
		return NewError(
			InvalidOptionsError,
			method+" options.delimiter must be a valid CSV delimiter",
		)
	}

	if !utf8.ValidRune(r) {
		return NewError(
			InvalidOptionsError,
			method+" options.delimiter must be a valid CSV delimiter",
		)
	}

	return nil
}

// parseImportCSVOptionalHasHeader parses and validates a single options field.
func parseImportCSVOptionalHasHeader(parsed *importCSVOptions, optionsObj *sobek.Object) error {
	hasHeader, hasHeaderSet, err := parseOptionalBoolOption("importCSV", "hasHeader", optionsObj.Get("hasHeader"))
	if err != nil {
		return err
	}

	if hasHeaderSet {
		parsed.HasHeader = hasHeader
	}

	return nil
}

// validateImportCSVKeyColumn validates user-supplied input.
func validateImportCSVKeyColumn(parsed importCSVOptions) error {
	if parsed.HasHeader {
		return nil
	}

	keyIndex, parseErr := strconv.Atoi(parsed.KeyColumn)
	if parseErr != nil || keyIndex < 0 {
		return NewError(
			InvalidOptionsError,
			"importCSV keyColumn must be a non-negative integer when hasHeader=false",
		)
	}

	return nil
}

// parseImportCSVOptionalLimit parses and validates a single options field.
func parseImportCSVOptionalLimit(parsed *importCSVOptions, optionsObj *sobek.Object) error {
	limit, limitSet, err := parseOptionalInt64Option("importCSV", "limit", optionsObj.Get("limit"))
	if err != nil {
		return err
	}

	if !limitSet {
		return nil
	}

	if limit < 0 {
		return NewError(
			InvalidOptionsError,
			"importCSV options.limit must be non-negative",
		)
	}

	if err := rejectIfAbove("importCSV", "limit", limit, MaxImportCSVLimit); err != nil {
		return err
	}

	parsed.Limit = limit

	return nil
}

// parseImportCSVOptionalBatchSize parses and validates a single options field.
func parseImportCSVOptionalBatchSize(parsed *importCSVOptions, optionsObj *sobek.Object) error {
	batchSize, batchSizeSet, err := parseOptionalInt64Option("importCSV", "batchSize", optionsObj.Get("batchSize"))
	if err != nil {
		return err
	}

	if !batchSizeSet {
		return nil
	}

	if batchSize < 0 {
		return NewError(
			InvalidOptionsError,
			"importCSV options.batchSize must be non-negative",
		)
	}

	if err := rejectIfAbove("importCSV", "batchSize", batchSize, MaxCSVBatchSize); err != nil {
		return err
	}

	parsed.BatchSize = batchSize

	return nil
}
