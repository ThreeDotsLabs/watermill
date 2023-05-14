/*

Package jsonendpoint provides a generic adapter that converts a validated JSON request into a [message.Message] published to a certain topic. Use together with an HTTP router to build gateways to any [message.Publisher].

# Usage Example

Imagine a fairly standard situation of collecting newsletter signups from a Restful API:

	type NewsletterSignup struct {
		Name  string
		Email string
	}

    // Validate satisfies [Validator] interface.
	func (n *NewsletterSignup) Validate() error {
		if n.Name == "" || n.Email == "" {
			return errors.New("newsletter signup requires both name and email address")
		}
		return nil
	}

	func main() {
		pubSub := gochannel.NewGoChannel(gochannel.Config{
			OutputChannelBuffer: 100,
			Persistent:          true,
		},
			watermill.NewStdLogger(true, true),
		)

		endpoint := New(
			1024*1024, // HTTP readLimit
			func(m *NewsletterSignup) (*message.Message, error) { // converter
				payload, err := json.Marshal(m)
				if err != nil {
					return nil, fmt.Errorf("failed to encode: %w", err)
				}
				return message.NewMessage(watermill.NewUUID(), payload), nil
			},
			"newsletter/signup", // Watermill topic
			pubSub)

		// ... setup HTTP server and router
		router.Post("/api/v1/newsletter/signup", endpoint)
	}

JSON HTTP post requests that hit "/api/v1/newsletter/signup" will get parsed, validated, and converted into a [message.Message].
*/
package jsonendpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Validatable is a generic interface that requires type T to be a pointer and implement the Validate method. It complements the adapter definitions. See <https://stackoverflow.com/questions/72090387/what-is-the-generic-type-for-a-pointer-that-implements-an-interface>.
type Validatable[T any] interface {
	*T
	Validate() error
}

// New creates an adapter that converts an HTTP request to a [message.Message] and sends it to the [message.Publisher] topic. Enforces message validation on the generic type.
func New[T any, P Validatable[T]](readLimit int64, converter func(P) (*message.Message, error), topic string, p message.Publisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var (
			in  = new(T) //*IN
			err error
		)
		defer func() {
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"Error": err.Error(),
				})
			}
		}()

		if r.Method != http.MethodPost {
			err = fmt.Errorf("request method %q is not supported", r.Method)
			return
		}

		reader := http.MaxBytesReader(w, r.Body, readLimit)
		defer reader.Close()

		err = json.NewDecoder(reader).Decode(in)
		if err != nil {
			r.Body.Close()
			err = errors.New("JSON decoding failure: " + err.Error())
			return
		}
		r.Body.Close()

		if in == nil {
			err = errors.New("no post data provided")
			return
		}

		if err = P(in).Validate(); err != nil {
			err = fmt.Errorf("failed to validate: %w", err)
			return
		}

		message, err := converter(in)
		if err != nil {
			err = fmt.Errorf("failed to construct a message: %w", err)
			return
		}
		message.SetContext(r.Context())

		if err = p.Publish(topic, message); err != nil {
			err = fmt.Errorf("publisher rejected the message: %w", err)
			return
		}

		response, err := json.Marshal(map[string]string{
			"UUID": message.UUID,
		})
		if err != nil {
			r.Body.Close()
			err = errors.New("JSON encoding failure: " + err.Error())
			return
		}

		_, err = w.Write(response)
	}
}
