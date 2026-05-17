package kv

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
)

type importCSVOptions struct {
	FileName  string
	KeyColumn string
	Delimiter rune
	HasHeader bool
	Limit     int64
	BatchSize int64
}

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

func parseImportCSVOptionalDelimiter(parsed *importCSVOptions, optionsObj *sobek.Object) error {
	delimiter, delimiterSet, err := parseOptionalStringOption("importCSV", "delimiter", optionsObj.Get("delimiter"))
	if err != nil {
		return err
	}

	if !delimiterSet {
		return nil
	}

	r, err := parseSingleRuneDelimiter(delimiter)
	if err != nil {
		return err
	}

	parsed.Delimiter = r

	return nil
}

func parseSingleRuneDelimiter(delimiter string) (rune, error) {
	if delimiter == "" {
		return 0, NewError(
			InvalidOptionsError,
			"importCSV options.delimiter must be exactly one character",
		)
	}

	r, size := utf8.DecodeRuneInString(delimiter)
	if (r == utf8.RuneError && size == 0) || size != len(delimiter) {
		return 0, NewError(
			InvalidOptionsError,
			"importCSV options.delimiter must be exactly one character",
		)
	}

	return r, nil
}

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
