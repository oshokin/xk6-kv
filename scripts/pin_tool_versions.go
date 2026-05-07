// Package main updates pinned tool versions across repository automation files.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os/exec"
	"path/filepath"
	"regexp"

	"github.com/spf13/afero"
)

type (
	// resolvedInputs groups version and root data so call sites do not pass loose tuples.
	resolvedInputs struct {
		// XK6 stores the module metadata used for xk6 and Go version pins.
		XK6 *moduleInfo
		// Lint stores the module metadata used for golangci-lint pins.
		Lint *moduleInfo
		// Root is the absolute repository root where target files are located.
		Root string
	}

	// fileContent keeps file bytes with their original permissions for rewrites.
	fileContent struct {
		// Data is the original file content before replacements.
		Data []byte
		// Mode is preserved when writing updated file content.
		Mode fs.FileMode
	}

	// moduleInfo mirrors the subset of `go list -m -json` fields used by this script.
	moduleInfo struct {
		// Path is the module path returned by go list.
		Path string `json:"Path"`
		// Version is the resolved module version returned by go list.
		Version string `json:"Version"`
		// GoVersion is present for modules that declare a Go toolchain version.
		GoVersion string `json:"GoVersion"`
		// Query is the original module query echoed by go list.
		Query string `json:"Query"`
	}

	// fileUpdate describes all replacements applied to one target file.
	fileUpdate struct {
		// Path is the target file to read and possibly rewrite.
		Path string
		// Replacements are applied sequentially to Path.
		Replacements []*replacement
	}

	// replacement describes one exact regex replacement within a target file.
	replacement struct {
		// Re identifies the existing line that should be replaced.
		Re *regexp.Regexp
		// With is the complete replacement line.
		With string
		// Want is the exact number of matches required for a safe edit.
		Want int
	}
)

const (
	// dryRunFlagName is the CLI flag that previews edits without touching files.
	dryRunFlagName = "dry-run"
	// dryRunFlagUsage explains that dry-run mode only reports pending writes.
	dryRunFlagUsage = "print changes without writing files"
	// rootFlagName is the CLI flag that overrides repository root discovery.
	rootFlagName = "root"
	// rootFlagUsage explains how the script chooses the repository root.
	rootFlagUsage = "repository root (defaults to auto-detect)"

	// xk6LatestModuleQuery asks Go to resolve the latest xk6 module version.
	xk6LatestModuleQuery = "go.k6.io/xk6@latest"
	// golangCILintLatestModuleQuery asks Go to resolve the latest golangci-lint module version.
	golangCILintLatestModuleQuery = "github.com/golangci/golangci-lint/v2@latest"
	// goExecutableName is the command used for module metadata lookup.
	goExecutableName = "go"
	// goListCommand is the go subcommand that emits module metadata.
	goListCommand = "list"
	// goListModuleFlag scopes go list to module metadata instead of packages.
	goListModuleFlag = "-m"
	// goListJSONFlag requests machine-readable module metadata from go list.
	goListJSONFlag = "-json"

	// currentDirPath is resolved to an absolute path before walking parents.
	currentDirPath = "."
	// goModFileName marks the repository root for this script.
	goModFileName = "go.mod"
	// githubDirName names GitHub automation configuration directories.
	githubDirName = ".github"
	// workflowsDirName names the GitHub Actions workflow directory.
	workflowsDirName = "workflows"
	// actionsDirName names the GitHub Actions composite actions directory.
	actionsDirName = "actions"
	// releaseActionDirName names this repository's release composite action.
	releaseActionDirName = "release"
	// extensionCheckWorkflowFile is the workflow that pins Go, xk6, and golangci-lint.
	extensionCheckWorkflowFile = "extension-check.yaml"
	// releaseActionFile is the composite action that pins Go and xk6.
	releaseActionFile = "action.yml"
	// taskfileName is updated so local lint tasks use the same pinned tools.
	taskfileName = "taskfile.yaml"

	// expectedSingleReplacement protects against silently editing duplicate or missing config lines.
	expectedSingleReplacement = 1

	// golangCILintVersionPattern matches the workflow env var for golangci-lint.
	golangCILintVersionPattern = `(?m)^  GOLANGCI_LINT_VERSION: ".*"$`
	// goVersionPattern matches the workflow env var for Go.
	goVersionPattern = `(?m)^  GO_VERSION: ".*"$`
	// xk6VersionPattern matches the workflow env var for xk6.
	xk6VersionPattern = `(?m)^  XK6_VERSION: ".*"$`
	// goVersionMatrixPattern matches the workflow build matrix Go version.
	goVersionMatrixPattern = `(?m)^        go-version: \[".*"\]$`
	// releaseGoVersionPattern matches the release action Go env export.
	releaseGoVersionPattern = `(?m)^        echo "GO_VERSION=[^"]+" >> \$GITHUB_ENV$`
	// releaseXK6VersionPattern matches the release action xk6 env export.
	releaseXK6VersionPattern = `(?m)^        echo "XK6_VERSION=[^"]+" >> \$GITHUB_ENV$`
	// taskfileGolangCITagPattern matches the taskfile golangci-lint tag.
	taskfileGolangCITagPattern = `(?m)^  GOLANGCI_TAG: ".*"$`
	// taskfileXK6TagPattern matches the taskfile xk6 tag.
	taskfileXK6TagPattern = `(?m)^  XK6_TAG: ".*"$`

	// golangCILintVersionLine renders the workflow env var for golangci-lint.
	golangCILintVersionLine = `  GOLANGCI_LINT_VERSION: "%s"`
	// goVersionLine renders the workflow env var for Go.
	goVersionLine = `  GO_VERSION: "%s"`
	// xk6VersionLine renders the workflow env var for xk6.
	xk6VersionLine = `  XK6_VERSION: "%s"`
	// goVersionMatrixLine renders the workflow build matrix Go version.
	goVersionMatrixLine = `        go-version: ["%s"]`
	// releaseGoVersionLine renders the release action Go env export.
	releaseGoVersionLine = `        echo "GO_VERSION=%s" >> $GITHUB_ENV`
	// releaseXK6VersionLine renders the release action xk6 env export.
	releaseXK6VersionLine = `        echo "XK6_VERSION=%s" >> $GITHUB_ENV`
	// taskfileGolangCITagLine renders the taskfile golangci-lint tag.
	taskfileGolangCITagLine = `  GOLANGCI_TAG: "%s"`
	// taskfileXK6TagLine renders the taskfile xk6 tag.
	taskfileXK6TagLine = `  XK6_TAG: "%s"`

	// changedDryRunStatusPrefix prefixes files that would change in dry-run mode.
	changedDryRunStatusPrefix = "would update:"
	// changedStatusPrefix prefixes files rewritten by the script.
	changedStatusPrefix = "updated:  "
	// unchangedStatusPrefix prefixes files that already contain current versions.
	unchangedStatusPrefix = "no changes:"
	// doneMessage is printed after all configured updates are processed.
	doneMessage = "Done. Review changes and commit if desired."
	// statusLineFormat renders a status prefix followed by a target file path.
	statusLineFormat = "%s %s\n"
	// resolvedXK6VersionFormat reports the resolved xk6 module version.
	resolvedXK6VersionFormat = "Resolved xk6@latest -> %s\n"
	// resolvedXK6GoVersionFormat reports the Go version required by xk6.
	resolvedXK6GoVersionFormat = "Resolved go.k6.io/xk6 Go version -> %s\n"
	// resolvedGolangCILintVersionFormat reports the resolved golangci-lint module version.
	resolvedGolangCILintVersionFormat = "Resolved golangci-lint@latest -> %s\n"

	// emptyXK6VersionErrorFormat identifies an invalid go list response for xk6.
	emptyXK6VersionErrorFormat = "go list returned empty xk6 version (query=%q)"
	// emptyXK6GoVersionErrorFormat identifies an invalid go list response for xk6's Go version.
	emptyXK6GoVersionErrorFormat = "go list returned empty xk6 go version (query=%q)"
	// emptyGolangCILintVersionErrorFormat identifies an invalid go list response for golangci-lint.
	emptyGolangCILintVersionErrorFormat = "go list returned empty golangci-lint version (query=%q)"
	// writeFileErrorFormat wraps write failures with the target path.
	writeFileErrorFormat = "write %s: %w"
	// replacementCountErrorFormat reports patterns that match too many or too few lines.
	replacementCountErrorFormat = "%s: expected %d replacements for %q, got %d"
	// statFileErrorFormat wraps stat failures with the target path.
	statFileErrorFormat = "stat %s: %w"
	// readFileErrorFormat wraps read failures with the target path.
	readFileErrorFormat = "read %s: %w"
	// resolveCurrentDirectoryErrorFormat wraps failure to resolve the process directory.
	resolveCurrentDirectoryErrorFormat = "resolve current directory: %w"
	// repoRootNotFoundErrorMessage explains why repository root discovery failed.
	repoRootNotFoundErrorMessage = "unable to locate repo root (no go.mod found)"
	// goListErrorFormat wraps go list failures with stderr/stdout output.
	goListErrorFormat = "go list %s: %w\n%s"
	// parseGoListOutputErrorFormat wraps malformed go list JSON output.
	parseGoListOutputErrorFormat = "parse go list output for %s: %w"
)

// main parses CLI flags and runs the version pinning workflow.
func main() {
	dryRun := flag.Bool(dryRunFlagName, false, dryRunFlagUsage)
	rootFlag := flag.String(rootFlagName, "", rootFlagUsage)

	flag.Parse()

	if err := run(afero.NewOsFs(), *rootFlag, *dryRun); err != nil {
		log.Fatal(err)
	}
}

// run resolves current tool versions and applies them to configured files.
func run(files afero.Fs, rootInput string, dryRun bool) error {
	inputs, err := resolveInputs(files, rootInput)
	if err != nil {
		return err
	}

	printResolvedVersions(inputs)

	return applyUpdates(files, buildUpdates(inputs), dryRun)
}

// resolveInputs collects the repository root and module versions needed for all updates.
func resolveInputs(files afero.Fs, rootInput string) (*resolvedInputs, error) {
	root, err := resolveRepoRoot(files, rootInput)
	if err != nil {
		return nil, err
	}

	xk6, err := goListModule(xk6LatestModuleQuery)
	if err != nil {
		return nil, err
	}

	lint, err := goListModule(golangCILintLatestModuleQuery)
	if err != nil {
		return nil, err
	}

	if xk6.Version == "" {
		return nil, fmt.Errorf(emptyXK6VersionErrorFormat, xk6.Query)
	}

	if xk6.GoVersion == "" {
		return nil, fmt.Errorf(emptyXK6GoVersionErrorFormat, xk6.Query)
	}

	if lint.Version == "" {
		return nil, fmt.Errorf(emptyGolangCILintVersionErrorFormat, lint.Query)
	}

	return &resolvedInputs{
		XK6:  xk6,
		Lint: lint,
		Root: root,
	}, nil
}

// printResolvedVersions writes the version lookup summary for the operator.
func printResolvedVersions(inputs *resolvedInputs) {
	fmt.Printf(resolvedXK6VersionFormat, inputs.XK6.Version)
	fmt.Printf(resolvedXK6GoVersionFormat, inputs.XK6.GoVersion)
	fmt.Printf(resolvedGolangCILintVersionFormat, inputs.Lint.Version)
	fmt.Println()
}

// buildUpdates creates all file rewrite plans from resolved module versions.
func buildUpdates(inputs *resolvedInputs) []*fileUpdate {
	return []*fileUpdate{
		{
			Path: filepath.Join(inputs.Root, githubDirName, workflowsDirName, extensionCheckWorkflowFile),
			Replacements: []*replacement{
				{
					Re:   regexp.MustCompile(golangCILintVersionPattern),
					With: fmt.Sprintf(golangCILintVersionLine, inputs.Lint.Version),
					Want: expectedSingleReplacement,
				},
				{
					Re:   regexp.MustCompile(goVersionPattern),
					With: fmt.Sprintf(goVersionLine, inputs.XK6.GoVersion),
					Want: expectedSingleReplacement,
				},
				{
					Re:   regexp.MustCompile(xk6VersionPattern),
					With: fmt.Sprintf(xk6VersionLine, inputs.XK6.Version),
					Want: expectedSingleReplacement,
				},
				{
					Re:   regexp.MustCompile(goVersionMatrixPattern),
					With: fmt.Sprintf(goVersionMatrixLine, inputs.XK6.GoVersion),
					Want: expectedSingleReplacement,
				},
			},
		},
		{
			Path: filepath.Join(inputs.Root, githubDirName, actionsDirName, releaseActionDirName, releaseActionFile),
			Replacements: []*replacement{
				{
					Re:   regexp.MustCompile(releaseGoVersionPattern),
					With: fmt.Sprintf(releaseGoVersionLine, inputs.XK6.GoVersion),
					Want: expectedSingleReplacement,
				},
				{
					Re:   regexp.MustCompile(releaseXK6VersionPattern),
					With: fmt.Sprintf(releaseXK6VersionLine, inputs.XK6.Version),
					Want: expectedSingleReplacement,
				},
			},
		},
		{
			Path: filepath.Join(inputs.Root, taskfileName),
			Replacements: []*replacement{
				{
					Re:   regexp.MustCompile(taskfileGolangCITagPattern),
					With: fmt.Sprintf(taskfileGolangCITagLine, inputs.Lint.Version),
					Want: expectedSingleReplacement,
				},
				{
					Re:   regexp.MustCompile(taskfileXK6TagPattern),
					With: fmt.Sprintf(taskfileXK6TagLine, inputs.XK6.Version),
					Want: expectedSingleReplacement,
				},
			},
		},
	}
}

// applyUpdates executes each file update plan and prints per-file status.
func applyUpdates(files afero.Fs, updates []*fileUpdate, dryRun bool) error {
	for _, update := range updates {
		changed, err := applyFileUpdate(files, update, dryRun)
		if err != nil {
			return err
		}

		switch {
		case changed && dryRun:
			fmt.Printf(statusLineFormat, changedDryRunStatusPrefix, update.Path)
		case changed:
			fmt.Printf(statusLineFormat, changedStatusPrefix, update.Path)
		default:
			fmt.Printf(statusLineFormat, unchangedStatusPrefix, update.Path)
		}
	}

	fmt.Println()
	fmt.Println(doneMessage)

	return nil
}

// applyFileUpdate reads one file, applies its replacements, and writes it when changed.
func applyFileUpdate(files afero.Fs, update *fileUpdate, dryRun bool) (bool, error) {
	original, err := readFileWithMode(files, update.Path)
	if err != nil {
		return false, err
	}

	updated := original.Data
	for _, r := range update.Replacements {
		next, err := replaceExactly(updated, r.Re, r.With, r.Want, update.Path)
		if err != nil {
			return false, err
		}

		updated = next
	}

	if bytes.Equal(original.Data, updated) {
		return false, nil
	}

	if dryRun {
		return true, nil
	}

	if err := afero.WriteFile(
		files,
		update.Path,
		updated,
		original.Mode,
	); err != nil {
		return false, fmt.Errorf(writeFileErrorFormat, update.Path, err)
	}

	return true, nil
}

// replaceExactly replaces text only when the pattern matches the expected count.
func replaceExactly(input []byte, re *regexp.Regexp, replacement string, want int, fileName string) ([]byte, error) {
	matches := re.FindAllIndex(input, -1)
	if got := len(matches); got != want {
		return nil, fmt.Errorf(replacementCountErrorFormat, fileName, want, re.String(), got)
	}

	return re.ReplaceAllLiteral(input, []byte(replacement)), nil
}

// readFileWithMode returns file contents together with mode bits for preserving permissions.
func readFileWithMode(files afero.Fs, path string) (*fileContent, error) {
	fi, err := files.Stat(path)
	if err != nil {
		return nil, fmt.Errorf(statFileErrorFormat, path, err)
	}

	data, err := afero.ReadFile(files, path)
	if err != nil {
		return nil, fmt.Errorf(readFileErrorFormat, path, err)
	}

	return &fileContent{
		Data: data,
		Mode: fi.Mode(),
	}, nil
}

// resolveRepoRoot finds the nearest parent directory containing go.mod.
func resolveRepoRoot(files afero.Fs, explicit string) (string, error) {
	if explicit != "" {
		return filepath.Abs(explicit)
	}

	cwd, err := filepath.Abs(currentDirPath)
	if err != nil {
		return "", fmt.Errorf(resolveCurrentDirectoryErrorFormat, err)
	}

	dir := cwd
	for {
		if fileExists(files, filepath.Join(dir, goModFileName)) {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New(repoRootNotFoundErrorMessage)
		}

		dir = parent
	}
}

// fileExists reports whether path can be statted through the configured filesystem.
func fileExists(files afero.Fs, path string) bool {
	_, err := files.Stat(path)
	return err == nil
}

// goListModule resolves one module query using `go list -m -json`.
func goListModule(module string) (*moduleInfo, error) {
	//nolint:gosec // module queries are fixed by this tool, not user-provided shell input.
	cmd := exec.CommandContext(
		context.Background(),
		goExecutableName,
		goListCommand,
		goListModuleFlag,
		goListJSONFlag,
		module,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf(goListErrorFormat, module, err, string(out))
	}

	var info moduleInfo
	if err := json.Unmarshal(out, &info); err != nil {
		return nil, fmt.Errorf(parseGoListOutputErrorFormat, module, err)
	}

	return &info, nil
}
