package model

import (
	"os"

	"github.com/chanmaoganda/go-project-template/config"
	"gopkg.in/yaml.v3"
)

type Worker struct {
	config.Settings
}

func NewWorker() (*Worker, error) {
	yamlFile, err := os.ReadFile("settings.yml")
	if err != nil {
		return nil, err
	}

	var settings config.Settings

	err = yaml.Unmarshal(yamlFile, &settings)
	if err != nil {
		return nil, err
	}
	return &Worker{
		Settings: settings,
	}, nil
}
