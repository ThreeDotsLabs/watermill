package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/fatih/color"
	yaml "gopkg.in/yaml.v2"
)

type Config struct {
	ValidationCmd  string `yaml:"validation_cmd"`
	TeardownCmd    string `yaml:"teardown_cmd"`
	Timeout        int    `yaml:"timeout"`
	ExpectedOutput string `yaml:"expected_output"`
}

func (c *Config) LoadFrom(path string) error {
	file, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(file, c)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	walkErr := filepath.Walk("../../", func(exampleConfig string, f os.FileInfo, _ error) error {
		matches, _ := filepath.Match(".validate_example*.yml", f.Name())
		if !matches {
			return nil
		}

		exampleDirectory := filepath.Dir(exampleConfig)

		fmt.Printf("validating %s\n", exampleDirectory)

		err := validate(exampleConfig)
		if err != nil {
			return fmt.Errorf("validation for %s failed, err: %v", exampleDirectory, err)
		}

		return nil
	})
	if walkErr != nil {
		panic(walkErr)
	}

}

func validate(path string) error {
	config := &Config{}
	err := config.LoadFrom(path)
	if err != nil {
		return fmt.Errorf("could not load config, err: %v", err)
	}

	dirName := filepath.Base(filepath.Dir(path))

	fmt.Print("\n\n")
	fmt.Println("Validating example:", dirName)
	fmt.Println("Waiting for output: ", color.GreenString(config.ExpectedOutput))

	cmdAndArgs := strings.Fields(config.ValidationCmd)
	validationCmd := exec.Command(cmdAndArgs[0], cmdAndArgs[1:]...)
	validationCmd.Dir = filepath.Dir(path)
	defer func() {
		if config.TeardownCmd == "" {
			return
		}
		cmdAndArgs := strings.Fields(config.TeardownCmd)
		teardownCmd := exec.Command(cmdAndArgs[0], cmdAndArgs[1:]...)
		teardownCmd.Dir = filepath.Dir(path)
		_ = teardownCmd.Run()
	}()

	stdout, err := validationCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("could not attach to stdout, err: %v", err)
	}

	stderr, err := validationCmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("could not attach to stderr, err: %v", err)
	}

	fmt.Printf("running: %v\n", validationCmd.Args)

	err = validationCmd.Start()
	if err != nil {
		return fmt.Errorf("could not start validation, err: %v", err)
	}

	defer func() {
		err := validationCmd.Process.Kill()
		if err != nil {
			fmt.Printf("could not kill process in %s, err: %v\n", dirName, err)
		}
	}()

	success := make(chan error)

	go func() {
		io.MultiReader()

		output := bufio.NewReader(io.MultiReader(stdout, stderr))
		for {
			line, _, err := output.ReadLine()
			if err != nil {
				if err == io.EOF {
					break
				}
			}

			fmt.Printf("[%s] > %s\n", color.CyanString(dirName), string(line))

			ok, _ := regexp.Match(config.ExpectedOutput, line)
			if ok {
				success <- nil
				return
			}
		}
		success <- fmt.Errorf("could not find expected output: %s", config.ExpectedOutput)
	}()

	select {
	case err := <-success:
		return err
	case <-time.After(time.Duration(config.Timeout) * time.Second):
		return fmt.Errorf("validation command timed out")
	}
}
