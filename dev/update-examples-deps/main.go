package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

func main() {
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

func goModTidy(dir string, file string) error {
	cmd2 := exec.Command("go", "mod", "tidy")
	cmd2.Dir = dir
	cmd2.Stderr = os.Stderr
	cmd2.Stdout = os.Stdout

	return cmd2.Run()
}

func updateWatermill(dir string, file string) error {
	cmd := exec.Command("go", "get", "-u", "github.com/ThreeDotsLabs/watermill")
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Run()

	return err
}
