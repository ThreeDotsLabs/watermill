package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// simple script to consolidate all gomods to one gomod
// required for GolangCI linter
func main() {
	bigFatGomod := ""

	for _, fileName := range getGomods() {
		dir := filepath.Dir(fileName)
		if dir == "." {
			continue
		}

		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}

		fileMod := ""

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			txt := scanner.Text()
			if strings.HasPrefix(txt, "go ") {
				continue
			}
			if strings.HasPrefix(txt, "module ") {
				continue
			}

			fileMod += txt + "\n"
		}

		if err := scanner.Err(); err != nil {
			panic(err)
		}

		if fileMod != "" {
			bigFatGomod += "// " + fileName + "\n"
			bigFatGomod += fileMod + "\n"
		}

		_ = file.Close()

		// gomod is stupid, and go vendor removes all deps that are not needed
		// (and they are not needed if they are already meet in sub go.mods)
		if err := os.Remove(fileName); err != nil {
			panic(err)
		}
	}

	fmt.Println(bigFatGomod)
}

func getGomods() []string {
	var fileList []string

	err := filepath.Walk(".", func(path string, f os.FileInfo, err error) error {
		if strings.Contains(path, "/vendor/") {
			return nil
		}

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
