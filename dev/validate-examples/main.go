package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
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
	err := filepath.Walk(".", func(path string, f os.FileInfo, err error) error {
		matches, err := filepath.Match(".validate_example*.yml", f.Name())
		if matches {
			ok, err := validate(path)
			if err != nil {
				fmt.Printf("could not validate %s, err: %v\n", path, err)
			} else {
				if ok {
					fmt.Println("validation succeeded")
				} else {
					fmt.Println("validation failed")
				}
			}

		}
		return nil
	})

	if err != nil {
		panic("could not Walk path")
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
		cmdAndArgs := strings.Fields(config.TeardownCmd)
		teardownCmd := exec.Command(cmdAndArgs[0], cmdAndArgs[1:]...)
		teardownCmd.Dir = filepath.Dir(path)
		teardownCmd.Run()
	}()

	stdout, err := validationCmd.StdoutPipe()
	if err != nil {
		return false, fmt.Errorf("could not attach to stdout, err: %v", err)
	}

	err = validationCmd.Start()
	if err != nil {
		return false, fmt.Errorf("could not start validation, err: %v", err)
	}

	success := make(chan bool)

	go func() {
		outputLines := bufio.NewReader(stdout)
		for {
			line, _, err := outputLines.ReadLine()
			if err != nil {
				if err == io.EOF {
					break
				}
			}
			if strings.Contains(string(line), config.ExpectedOutput) {
				success <- true
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
