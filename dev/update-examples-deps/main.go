package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var latestGoVersion string

func main() {
	latestGoVersion = getLatestGoVersionFromWebsite()

	const workers = 5

	wg := sync.WaitGroup{}
	wg.Add(workers)

	files := make(chan string)

	for i := 0; i < workers; i++ {
		go func() {
			for file := range files {
				dir := filepath.Dir(file)
				if dir == "." {
					continue
				}

				fmt.Println("update of", file, "@", dir)

				if err := replaceGoInDockerCompose(dir); err != nil {
					panic(err)
				}

				if err := updateDeps(dir, file); err != nil {
					panic(err)
				}

				if err := updateWatermill(dir, file); err != nil {
					panic(fmt.Sprintf("failed to update %s: %s", file, err))
				}

				if err := goModTidy(dir, file); err != nil {
					panic(err)
				}
			}

			wg.Done()
		}()
	}

	for _, file := range getGomods() {
		files <- file
	}
	close(files)

	wg.Wait()
}

func getGomods() []string {
	var fileList []string

	err := filepath.Walk(".", func(path string, f os.FileInfo, err error) error {
		if strings.Contains(path, "go.mod") {
			fileList = append(fileList, path)
		}
		return nil
	})

	if err != nil {
		panic(err)
	}

	return fileList
}

func getLatestGoVersionFromWebsite() string {
	resp, err := http.Get("https://go.dev/VERSION?m=text")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	version := strings.Split(string(out), "\n")[0]
	version = strings.TrimPrefix(version, "go")

	// we only want the major.minor version
	version = strings.Split(version, ".")[0] + "." + strings.Split(version, ".")[1]

	return version
}

func goModTidy(dir string, file string) error {
	cmd := []string{"go", "mod", "tidy", "-go=" + latestGoVersion}

	fmt.Println("\nrunning", cmd, "in", dir)

	cmd2 := exec.Command(cmd[0], cmd[1:]...)
	cmd2.Dir = dir
	cmd2.Stderr = os.Stderr
	cmd2.Stdout = os.Stdout

	return cmd2.Run()
}

// replaceGoInDockerCompose replaces the go version in the Dockerfile
// using Go (not sed)
func replaceGoInDockerCompose(dir string) error {
	dockerComposeFile := filepath.Join(dir, "docker-compose.yml")

	b, err := os.ReadFile(dockerComposeFile)
	// return if not exist
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return err
	}

	pattern := `golang:1\.[0-9]+(?:\.[0-9]+)?`
	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("failed to compile regex: %w", err)
	}

	newContent := re.ReplaceAllString(string(b), "golang:"+latestGoVersion)

	err = os.WriteFile(dockerComposeFile, []byte(newContent), 0644)
	if err != nil {
		return fmt.Errorf("failed to write updated docker-compose.yml: %w", err)
	}

	return nil
}

func updateWatermill(dir string, file string) error {
	c := []string{"go", "get", "-u", "github.com/ThreeDotsLabs/watermill@latest"}

	fmt.Println("\nrunning", c, "in", dir)

	cmd := exec.Command(c[0], c[1:]...)
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Run()

	return err
}

func updateDeps(dir string, file string) error {
	c := []string{"go", "get", "-u", "./..."}

	fmt.Println("\nrunning", c, "in", dir)

	cmd := exec.Command(c[0], c[1:]...)
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Run()

	return err
}
