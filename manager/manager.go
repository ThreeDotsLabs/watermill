package manager

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Driver interface {
	message.Publisher
	message.Subscriber
}

type Manager struct {
	Driver

	drivers map[string]Driver
}

func New(driver Driver) *Manager {
	return &Manager{
		Driver:  driver,
		drivers: make(map[string]Driver),
	}
}

func (m *Manager) Register(name string, driver Driver) {
	m.drivers[name] = driver
}

func (m *Manager) Use(names ...string) Driver {
	if len(names) <= 0 {
		return m.Driver
	}

	name := names[0]
	if driver, ok := m.drivers[name]; ok {
		return driver
	}

	panic(fmt.Errorf("watermill: unknown driver %s", name))
}
