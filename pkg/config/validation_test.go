package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Helper function to create a temporary file with specific content
func createTempFile(t *testing.T, dir, pattern, content string) (string, func()) {
	t.Helper()
	tempFile, err := ioutil.TempFile(dir, pattern)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	if content != "" {
		if _, err := tempFile.WriteString(content); err != nil {
			tempFile.Close()
			os.Remove(tempFile.Name())
			t.Fatalf("Failed to write to temp file: %v", err)
		}
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempFile.Name())
		t.Fatalf("Failed to close temp file: %v", err)
	}

	return tempFile.Name(), func() { os.Remove(tempFile.Name()) }
}

// Helper function to create a temporary directory
func createTempDir(t *testing.T, dir, pattern string) (string, func()) {
	t.Helper()
	tempDir, err := ioutil.TempDir(dir, pattern)
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return tempDir, func() { os.RemoveAll(tempDir) }
}

func TestValidateFileExists(t *testing.T) {
	t.Run("FileDoesNotExist", func(t *testing.T) {
		err := validateFileExists("non_existent_file.txt")
		if err == nil {
			t.Errorf("Expected an error for a non-existent file, but got nil")
		}
		if err != nil && !strings.Contains(err.Error(), "file does not exist") {
			t.Errorf("Expected error to contain 'file does not exist', but got: %v", err)
		}
	})

	t.Run("FileExistsAndReadable", func(t *testing.T) {
		filePath, cleanup := createTempFile(t, "", "readable_*.txt", "hello world")
		defer cleanup()

		err := validateFileExists(filePath)
		if err != nil {
			t.Errorf("Expected no error for a readable file, but got: %v", err)
		}
	})

	t.Run("EmptyFileExistsAndReadable", func(t *testing.T) {
		filePath, cleanup := createTempFile(t, "", "empty_*.txt", "")
		defer cleanup()

		err := validateFileExists(filePath)
		if err != nil {
			t.Errorf("Expected no error for an empty readable file, but got: %v", err)
		}
	})

	t.Run("FileExistsButNotReadable", func(t *testing.T) {
		filePath, cleanup := createTempFile(t, "", "unreadable_*.txt", "content")
		defer cleanup() // This cleanup will run after the test, potentially needing permissions fixed

		// Change file permissions to make it unreadable
		if err := os.Chmod(filePath, 0000); err != nil {
			t.Fatalf("Failed to change file permissions to unreadable: %v", err)
		}

		err := validateFileExists(filePath)
		if err == nil {
			t.Errorf("Expected an error for an unreadable file, but got nil")
		}
		if err != nil && (!strings.Contains(err.Error(), "file is not readable") && !strings.Contains(err.Error(), "permission denied")) {
			t.Errorf("Expected error to contain 'file is not readable' or 'permission denied', but got: %v", err)
		}

		// Change permissions back to allow cleanup, if necessary
		// This is important as defer cleanup() might fail otherwise.
		os.Chmod(filePath, 0600)
	})

	t.Run("PathIsADirectory", func(t *testing.T) {
		dirPath, cleanup := createTempDir(t, "", "test_dir_*")
		defer cleanup()

		err := validateFileExists(dirPath)
		if err == nil {
			t.Errorf("Expected an error when path is a directory, but got nil")
		}
		if err != nil && !strings.Contains(err.Error(), "path is a directory") {
			t.Errorf("Expected error to contain 'path is a directory', but got: %v", err)
		}
	})

	t.Run("EmptyFilename", func(t *testing.T) {
		err := validateFileExists("")
		if err == nil {
			t.Errorf("Expected an error for an empty filename, but got nil")
		}
		if err != nil && !strings.Contains(err.Error(), "filename cannot be empty") {
			t.Errorf("Expected error to contain 'filename cannot be empty', but got: %v", err)
		}
	})

	t.Run("TildeExpansion", func(t *testing.T) {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			t.Skipf("Skipping TildeExpansion test: could not get user home directory: %v", err)
		}

		// Create a dummy file directly in what we expect to be the home directory.
		// This avoids mocking os.UserHomeDir for this specific case.
		tempFileName := ".test_tilde_expansion_tempfile.txt"
		tempFilePath := filepath.Join(homeDir, tempFileName)

		// Create the file
		file, err := os.Create(tempFilePath)
		if err != nil {
			t.Fatalf("Failed to create temp file in home directory: %v", err)
		}
		file.Close() // Close immediately, we just need it to exist.
		defer os.Remove(tempFilePath) // Ensure cleanup

		err = validateFileExists(filepath.Join("~", tempFileName))
		if err != nil {
			t.Errorf("Expected no error for tilde-expanded path, but got: %v", err)
		}
	})

	t.Run("EnvironmentVariableExpansion", func(t *testing.T) {
		filePath, cleanup := createTempFile(t, "", "env_var_*.txt", "data")
		defer cleanup()

		envVarName := "TEST_VALIDATE_FILE_PATH"
		if err := os.Setenv(envVarName, filePath); err != nil {
			t.Fatalf("Failed to set environment variable: %v", err)
		}
		defer os.Unsetenv(envVarName) // Clean up environment variable

		// Test with $VAR style
		err := validateFileExists(fmt.Sprintf("$%s", envVarName))
		if err != nil {
			t.Errorf("Expected no error for env var path ($VAR), but got: %v", err)
		}

		// Test with ${VAR} style
		err = validateFileExists(fmt.Sprintf("${%s}", envVarName))
		if err != nil {
			t.Errorf("Expected no error for env var path (${VAR}), but got: %v", err)
		}

		// Test with non-existent env var
		err = validateFileExists("$NON_EXISTENT_ENV_VAR_FOR_TEST/somefile.txt")
		// Expecting it to try to access literally "$NON_EXISTENT_ENV_VAR_FOR_TEST/somefile.txt"
		// which should not exist.
		if err == nil {
			t.Errorf("Expected an error for a non-existent file due to non-existent env var, but got nil")
		}
		if err != nil && !strings.Contains(err.Error(), "file does not exist") {
			t.Errorf("Expected error for non-existent file (due to non-existent env var) to contain 'file does not exist', but got: %v", err)
		}
	})
}
