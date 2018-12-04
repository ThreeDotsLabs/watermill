package cqrs

type Query interface {
	QueryName() string
}
