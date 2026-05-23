package kv

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/afero"
)

// TestKVErrorNameTypescriptDeclarationsStayInSync verifies that kv error name typescript declarations stay in sync.
func TestKVErrorNameTypescriptDeclarationsStayInSync(t *testing.T) {
	t.Parallel()

	goNames := collectGoErrorNames(t, "errors.go")
	tsNames := collectTypescriptErrorNames(t, "../typescript/xk6-kv.d.ts")

	missingInTypescript := setDifference(goNames, tsNames)
	extraInTypescript := setDifference(tsNames, goNames)

	if len(missingInTypescript) > 0 || len(extraInTypescript) > 0 {
		t.Fatalf(
			"KVErrorName drift between kv/errors.go and typescript/xk6-kv.d.ts\nmissing in TypeScript: %v\nextra in TypeScript: %v",
			missingInTypescript,
			extraInTypescript,
		)
	}
}

// collectGoErrorNames collects go error names.
func collectGoErrorNames(t *testing.T, path string) map[string]struct{} {
	t.Helper()

	fileSet := token.NewFileSet()

	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	names := make(map[string]struct{})

	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			continue
		}

		collectGoErrorNamesFromConstDecl(names, genDecl)
	}

	return names
}

// collectGoErrorNamesFromConstDecl collects go error names from const decl.
func collectGoErrorNamesFromConstDecl(names map[string]struct{}, decl *ast.GenDecl) {
	for _, spec := range decl.Specs {
		valueSpec, ok := spec.(*ast.ValueSpec)
		if !ok {
			continue
		}

		for i, name := range valueSpec.Names {
			if !name.IsExported() || i >= len(valueSpec.Values) {
				continue
			}

			value, ok := stringLiteralValue(valueSpec.Values[i])
			if !ok || !strings.HasSuffix(value, "Error") {
				continue
			}

			names[value] = struct{}{}
		}
	}
}

// stringLiteralValue extracts a string literal value from an AST expression.
func stringLiteralValue(expr ast.Expr) (string, bool) {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}

	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}

	return value, true
}

// collectTypescriptErrorNames collects typescript error names.
func collectTypescriptErrorNames(t *testing.T, path string) map[string]struct{} {
	t.Helper()

	data, err := afero.ReadFile(afero.NewOsFs(), path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	match := regexp.MustCompile(`(?s)export type KVErrorName =(?P<body>.*?);`).FindSubmatch(data)
	if len(match) == 0 {
		t.Fatalf("%s: unable to find KVErrorName union", path)
	}

	names := make(map[string]struct{})
	for _, item := range regexp.MustCompile(`'([^']+)'`).FindAllSubmatch(match[1], -1) {
		names[string(item[1])] = struct{}{}
	}

	return names
}

// setDifference returns values present in left but absent in right.
func setDifference(left map[string]struct{}, right map[string]struct{}) []string {
	diff := make([]string, 0)

	for value := range left {
		if _, ok := right[value]; !ok {
			diff = append(diff, value)
		}
	}

	slices.Sort(diff)

	return diff
}
