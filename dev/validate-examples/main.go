package main

import (
	"bufio"
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
			validate(path)
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
		return false, err
	}

	cmdAndArgs := strings.Fields(config.ValidationCmd)

	cmd := exec.Command(cmdAndArgs[0], cmdAndArgs[1:]...)
	cmd.Dir = filepath.Dir(path)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return false, err
	}

	err = cmd.Start()
	if err != nil {
		return false, err
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

	var validationResult bool
	select {
	case validationResult = <-success:
	case <-time.After(time.Duration(config.Timeout) * time.Second):
		validationResult = false
	}

	cmdAndArgs = strings.Fields(config.ValidationCmd)
	down := exec.Command(cmdAndArgs[0], cmdAndArgs[1:]...)
	down.Dir = filepath.Dir(path)

	err = down.Run()
	if err != nil {
		return false, err
	}

	return validationResult, nil

}
