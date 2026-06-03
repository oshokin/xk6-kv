// Package main updates pinned tool versions across repository automation files.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	goversion "go/version"
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
		// XK6 stores the module metadata used for xk6 version pins.
		XK6 *moduleInfo
		// Lint stores the module metadata used for golangci-lint pins.
		Lint *moduleInfo
		// GoVersion stores the local go.mod Go directive used for Go version pins.
		GoVersion string
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
	// rootFlagName is the CLI flag that overrides repository root discovery.
	rootFlagName = "root"

	// xk6LatestModuleQuery asks Go to resolve the latest xk6 module version.
	xk6LatestModuleQuery = "go.k6.io/xk6@latest"
	// golangCILintLatestModuleQuery asks Go to resolve the latest golangci-lint module version.
	golangCILintLatestModuleQuery = "github.com/golangci/golangci-lint/v2@latest"
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
	// devcontainerDirName names the development container configuration directory.
	devcontainerDirName = ".devcontainer"
	// devcontainerFileName is updated so container Go matches go.mod.
	devcontainerFileName = "devcontainer.json"

	// expectedSingleReplacement protects against silently editing duplicate or missing config lines.
	expectedSingleReplacement = 1

	// goModVersionPattern matches the local Go directive in go.mod.
	goModVersionPattern = `(?m)^go ([0-9]+\.[0-9]+(?:\.[0-9]+)?)$`
	// goModVersionLine renders the local Go directive in go.mod.
	goModVersionLine = `go %s`
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
	// taskfileGoVersionPattern matches the taskfile Go toolchain pin.
	taskfileGoVersionPattern = `(?m)^  GO_VERSION: ".*"$`
	// taskfileXK6TagPattern matches the taskfile xk6 tag.
	taskfileXK6TagPattern = `(?m)^  XK6_TAG: ".*"$`
	// devcontainerGoVersionPattern matches the Go feature version in devcontainer.json.
	devcontainerGoVersionPattern = `(?m)^			"version": ".*"$`

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
	// taskfileGoVersionLine renders the taskfile Go toolchain pin.
	taskfileGoVersionLine = `  GO_VERSION: "%s"`
	// taskfileXK6TagLine renders the taskfile xk6 tag.
	taskfileXK6TagLine = `  XK6_TAG: "%s"`
	// devcontainerGoVersionLine renders the Go feature version in devcontainer.json.
	devcontainerGoVersionLine = `			"version": "%s"`
)

// main parses CLI flags and runs the version pinning workflow.
func main() {
	dryRun := flag.Bool(dryRunFlagName, false, "print changes without writing files")
	rootFlag := flag.String(rootFlagName, "", "repository root (defaults to auto-detect)")

	flag.Parse()

	if err := run(afero.NewOsFs(), *rootFlag, *dryRun); err != nil {
		log.Fatal(err)
	}
}

// run resolves current tool versions and applies them to configured files.
func run(files afero.Fs, rootInput string, dryRun bool) error {
	inputs, err := resolveInputs(files, rootInput, dryRun)
	if err != nil {
		return err
	}

	printResolvedVersions(inputs)

	return applyUpdates(files, buildUpdates(inputs), dryRun)
}

// resolveInputs collects the repository root and module versions needed for all updates.
func resolveInputs(files afero.Fs, rootInput string, dryRun bool) (*resolvedInputs, error) {
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
		return nil, fmt.Errorf("go list returned empty xk6 version (query=%q)", xk6.Query)
	}

	if lint.Version == "" {
		return nil, fmt.Errorf("go list returned empty golangci-lint version (query=%q)", lint.Query)
	}

	goVersion, err := reconcileGoModVersion(files, root, []*moduleInfo{xk6, lint}, dryRun)
	if err != nil {
		return nil, err
	}

	return &resolvedInputs{
		XK6:       xk6,
		Lint:      lint,
		GoVersion: goVersion,
		Root:      root,
	}, nil
}

// printResolvedVersions writes the version lookup summary for the operator.
func printResolvedVersions(inputs *resolvedInputs) {
	fmt.Printf("Resolved xk6@latest -> %s\n", inputs.XK6.Version)
	fmt.Printf("Resolved go.mod Go version -> %s\n", inputs.GoVersion)
	fmt.Printf("Resolved golangci-lint@latest -> %s\n", inputs.Lint.Version)
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
					With: fmt.Sprintf(goVersionLine, inputs.GoVersion),
					Want: expectedSingleReplacement,
				},
				{
					Re:   regexp.MustCompile(xk6VersionPattern),
					With: fmt.Sprintf(xk6VersionLine, inputs.XK6.Version),
					Want: expectedSingleReplacement,
				},
				{
					Re:   regexp.MustCompile(goVersionMatrixPattern),
					With: fmt.Sprintf(goVersionMatrixLine, inputs.GoVersion),
					Want: expectedSingleReplacement,
				},
			},
		},
		{
			Path: filepath.Join(inputs.Root, githubDirName, actionsDirName, releaseActionDirName, releaseActionFile),
			Replacements: []*replacement{
				{
					Re:   regexp.MustCompile(releaseGoVersionPattern),
					With: fmt.Sprintf(releaseGoVersionLine, inputs.GoVersion),
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
					Re:   regexp.MustCompile(taskfileGoVersionPattern),
					With: fmt.Sprintf(taskfileGoVersionLine, inputs.GoVersion),
					Want: expectedSingleReplacement,
				},
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
		{
			Path: filepath.Join(inputs.Root, devcontainerDirName, devcontainerFileName),
			Replacements: []*replacement{
				{
					Re:   regexp.MustCompile(devcontainerGoVersionPattern),
					With: fmt.Sprintf(devcontainerGoVersionLine, inputs.GoVersion),
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
			fmt.Printf("would update: %s\n", update.Path)
		case changed:
			fmt.Printf("updated:   %s\n", update.Path)
		default:
			fmt.Printf("no changes: %s\n", update.Path)
		}
	}

	fmt.Println()
	fmt.Println("Done. Review changes and commit if desired.")

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
		return false, fmt.Errorf("write %s: %w", update.Path, err)
	}

	return true, nil
}

// replaceExactly replaces text only when the pattern matches the expected count.
func replaceExactly(input []byte, re *regexp.Regexp, replacement string, want int, fileName string) ([]byte, error) {
	matches := re.FindAllIndex(input, -1)
	if got := len(matches); got != want {
		return nil, fmt.Errorf("%s: expected %d replacements for %q, got %d", fileName, want, re.String(), got)
	}

	return re.ReplaceAllLiteral(input, []byte(replacement)), nil
}

// readFileWithMode returns file contents together with mode bits for preserving permissions.
func readFileWithMode(files afero.Fs, path string) (*fileContent, error) {
	fi, err := files.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}

	data, err := afero.ReadFile(files, path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	return &fileContent{
		Data: data,
		Mode: fi.Mode(),
	}, nil
}

// readGoModVersion parses the local go.mod Go directive used for Go tool pins.
func readGoModVersion(files afero.Fs, path string) (string, error) {
	content, err := readFileWithMode(files, path)
	if err != nil {
		return "", err
	}

	matches := regexp.MustCompile(goModVersionPattern).FindSubmatch(content.Data)
	if len(matches) == 0 {
		return "", fmt.Errorf("%s: unable to find go directive", path)
	}

	return string(matches[1]), nil
}

// reconcileGoModVersion raises go.mod when resolved components require newer Go.
func reconcileGoModVersion(files afero.Fs, root string, modules []*moduleInfo, dryRun bool) (string, error) {
	goModPath := filepath.Join(root, goModFileName)

	current, err := readGoModVersion(files, goModPath)
	if err != nil {
		return "", err
	}

	required := requiredGoVersion(current, modules)
	if required == current {
		return current, nil
	}

	update := &fileUpdate{
		Path: goModPath,
		Replacements: []*replacement{
			{
				Re:   regexp.MustCompile(goModVersionPattern),
				With: fmt.Sprintf(goModVersionLine, required),
				Want: expectedSingleReplacement,
			},
		},
	}

	if _, err := applyFileUpdate(files, update, dryRun); err != nil {
		return "", err
	}

	fmt.Printf("Raised go.mod Go version -> %s\n", required)

	if dryRun {
		return required, nil
	}

	if err := runGoModTidy(root); err != nil {
		return "", err
	}

	return readGoModVersion(files, goModPath)
}

// requiredGoVersion returns the maximum Go version from go.mod and component metadata.
func requiredGoVersion(current string, modules []*moduleInfo) string {
	required := current

	for _, module := range modules {
		if module.GoVersion == "" {
			continue
		}

		if compareGoVersions(module.GoVersion, required) > 0 {
			required = module.GoVersion
		}
	}

	return required
}

// compareGoVersions compares Go language versions using Go's semantic version rules.
func compareGoVersions(left string, right string) int {
	return goversion.Compare("go"+left, "go"+right)
}

// resolveRepoRoot finds the nearest parent directory containing go.mod.
func resolveRepoRoot(files afero.Fs, explicit string) (string, error) {
	if explicit != "" {
		return filepath.Abs(explicit)
	}

	cwd, err := filepath.Abs(".")
	if err != nil {
		return "", fmt.Errorf("resolve current directory: %w", err)
	}

	dir := cwd
	for {
		if fileExists(files, filepath.Join(dir, goModFileName)) {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("unable to locate repo root (no go.mod found)")
		}

		dir = parent
	}
}

// fileExists reports whether path can be statted through the configured filesystem.
func fileExists(files afero.Fs, path string) bool {
	_, err := files.Stat(path)
	return err == nil
}

// runGoModTidy refreshes go.mod and go.sum after raising the Go directive.
func runGoModTidy(root string) error {
	cmd := exec.CommandContext(context.Background(), "go", "mod", "tidy")
	cmd.Dir = root

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("go mod tidy: %w\n%s", err, string(out))
	}

	return nil
}

// goListModule resolves one module query using `go list -m -json`.
func goListModule(module string) (*moduleInfo, error) {
	// #nosec G204 -- module query constants are fixed inside this tool, not user-controlled input.
	cmd := exec.CommandContext(
		context.Background(),
		"go",
		"list",
		"-m",
		"-json",
		module,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("go list %s: %w\n%s", module, err, string(out))
	}

	var info moduleInfo
	if err := json.Unmarshal(out, &info); err != nil {
		return nil, fmt.Errorf("parse go list output for %s: %w", module, err)
	}

	return &info, nil
}
