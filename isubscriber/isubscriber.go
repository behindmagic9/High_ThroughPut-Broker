package isubscriber

import (
	"observer/event"
)

type Isubscriber interface {
	Update(data *event.Event) error
	GetID() int64
}
