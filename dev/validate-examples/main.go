package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	yaml "gopkg.in/yaml.v2"
)

type Config struct {
	ValidationCmd  string `yaml:"validation_cmd"`
	TeardownCmd    string `yaml:"teardown_cmd"`
	Timeout        int    `yaml:"timeout"`
	ExpectedOutput string `yaml:"expected_output"`
}

func (c *Config) LoadFrom(path string) error {
	file, err := ioutil.ReadFile(path)
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
	walkErr := filepath.Walk(".", func(exampleConfig string, f os.FileInfo, _ error) error {
		matches, _ := filepath.Match(".validate_example*.yml", f.Name())
		if !matches {
			return nil
		}

		exampleDirectory := filepath.Dir(exampleConfig)

		fmt.Printf("validating %s\n", exampleDirectory)

		ok, err := validate(exampleConfig)

		if err != nil {
			fmt.Printf("validation for %s failed, err: %v\n", exampleDirectory, err)
			return nil
		}

		if ok {
			fmt.Printf("validation for %s succeeded\n", exampleDirectory)
			return nil
		}

		fmt.Printf("validation for %s failed\n", exampleDirectory)
		return nil
	})
	if walkErr != nil {
		panic(walkErr)
	}

}

func validate(path string) (bool, error) {
	config := &Config{}
	err := config.LoadFrom(path)
	if err != nil {
		return false, fmt.Errorf("could not load config, err: %v", err)
	}

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
		return false, fmt.Errorf("could not attach to stdout, err: %v", err)
	}

	fmt.Printf("running: %v\n", validationCmd.Args)

	err = validationCmd.Start()
	if err != nil {
		return false, fmt.Errorf("could not start validation, err: %v", err)
	}

	success := make(chan bool)

	go func() {
		output := bufio.NewReader(stdout)
		for {
			line, _, err := output.ReadLine()
			if err != nil {
				if err == io.EOF {
					break
				}
			}
			ok, _ := regexp.Match(config.ExpectedOutput, line)
			if ok {
				success <- true
				return
			}
		}
		success <- false
	}()

	select {
	case success := <-success:
		return success, nil
	case <-time.After(time.Duration(config.Timeout) * time.Second):
		return false, fmt.Errorf("validation command timed out")
	}
}
