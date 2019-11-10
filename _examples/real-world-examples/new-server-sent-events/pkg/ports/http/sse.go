package http

import (
	"net/http"

	"github.com/go-chi/render"

	"github.com/ThreeDotsLabs/watermill/message"
)

type GetterFunc func(w http.ResponseWriter, r *http.Request) (interface{}, bool)
type CheckerFunc func(r *http.Request, msg *message.Message) bool

type SSE struct {
	subscriber message.Subscriber
	topic      string

	getterFunc  GetterFunc
	checkerFunc CheckerFunc
}

func NewSSE(
	subscriber message.Subscriber,
	topic string,
	getterFunc GetterFunc,
	checkerFunc CheckerFunc,
) SSE {
	return SSE{
		subscriber:  subscriber,
		topic:       topic,
		getterFunc:  getterFunc,
		checkerFunc: checkerFunc,
	}
}

func (s SSE) Handler(w http.ResponseWriter, r *http.Request) {
	if render.GetAcceptedContentType(r) != render.ContentTypeEventStream {
		model, ok := s.getterFunc(w, r)
		if !ok {
			return
		}

		render.Respond(w, r, model)
		return
	}

	responsesChan := make(chan interface{})

	messages, err := s.subscriber.Subscribe(r.Context(), s.topic)
	if err != nil {
		w.WriteHeader(500)
		return
	}

	go func() {
		defer close(responsesChan)

		model, ok := s.getterFunc(w, r)
		if !ok {
			return
		}
		responsesChan <- model

		for msg := range messages {
			ok := s.checkerFunc(r, msg)
			if !ok {
				msg.Ack()
				continue
			}

			model, ok := s.getterFunc(w, r)
			if !ok {
				return
			}

			select {
			case <-r.Context().Done():
				break
			default:
			}

			responsesChan <- model
			msg.Ack()
		}
	}()

	render.Respond(w, r, responsesChan)
}
