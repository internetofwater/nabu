package testhelpers

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type GleanerExecutor struct {
	BinaryPath string
}

func BuildGleaner(ctx context.Context, binaryPath string, repoURL string, repoDir string) error {
	// Check if the binary already exists
	if _, err := os.Stat(binaryPath); err == nil {
		fmt.Println("Gleaner binary already exists, skipping build.")
		return nil
	}

	// Remove any existing repository directory
	if _, err := os.Stat(repoDir); err == nil {
		if err := os.RemoveAll(repoDir); err != nil {
			return fmt.Errorf("failed to remove existing repo directory: %w", err)
		}
	}

	// Clone the Gleaner repository
	fmt.Printf("Cloning repository from %s to %s\n", repoURL, repoDir)
	cloneCmd := exec.CommandContext(ctx, "git", "clone", repoURL, repoDir)
	cloneCmd.Stdout = os.Stdout
	cloneCmd.Stderr = os.Stderr
	if err := cloneCmd.Run(); err != nil {
		return fmt.Errorf("failed to clone repository: %w", err)
	}

	// Build the Gleaner binary
	fmt.Printf("Building Gleaner binary at %s\n", binaryPath)
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath)
	buildCmd.Dir = repoDir
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		return fmt.Errorf("failed to build Gleaner binary: %w", err)
	}

	// Ensure the binary is executable
	if err := os.Chmod(binaryPath, 0755); err != nil {
		return fmt.Errorf("failed to set executable permissions on Gleaner binary: %w", err)
	}

	return nil
}

func NewGleanerExecutor() (*GleanerExecutor, error) {
	ctx := context.Background()

	// Use system temp directory
	tempDir := os.TempDir()
	binaryPath := filepath.Join(tempDir, "gleaner")
	repoDir := filepath.Join(tempDir, "gleaner_repo")
	repoURL := "https://github.com/internetofwater/gleaner"

	// Build the Gleaner binary if needed
	if err := BuildGleaner(ctx, binaryPath, repoURL, repoDir); err != nil {
		return nil, fmt.Errorf("failed to initialize GleanerExecutor: %w", err)
	}

	return &GleanerExecutor{
		BinaryPath: binaryPath,
	}, nil
}

type RunResult struct {
	Stdout string
	Stderr string
}

func (g *GleanerExecutor) Run(cmd string) (RunResult, error) {
	ctx := context.Background()
	args := strings.Fields(cmd)

	cmdCtx := exec.CommandContext(ctx, g.BinaryPath, args...)

	// Buffers to capture stdout and stderr
	var stdoutBuf, stderrBuf bytes.Buffer
	cmdCtx.Stdout = &stdoutBuf
	cmdCtx.Stderr = &stderrBuf

	if err := cmdCtx.Run(); err != nil {
		return RunResult{}, fmt.Errorf("failed to run command '%s': %w", cmd, err)
	}

	return RunResult{
		Stdout: stdoutBuf.String(),
		Stderr: stderrBuf.String(),
	}, nil
}
