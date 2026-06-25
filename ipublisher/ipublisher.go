package ipublisher

import(
	"observer/event"
)

type Ipublisher interface{
	Publish(evnt *event.Event)
}
