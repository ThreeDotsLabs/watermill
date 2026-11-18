package cmd

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func TestAMQPConsumeDeclaresAndBindsTheQueue(t *testing.T) {
	testCases := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "queue only",
			args: []string{"-q", "orders"},
			want: []string{
				`queue.declare "orders" durable=true`,
			},
		},
		{
			name: "queue bound to an exchange",
			args: []string{"-q", "orders", "-x", "events", "--binding-key", "orders.created", "--exchange-type", "topic", "--durable=false"},
			want: []string{
				`queue.declare "orders" durable=false`,
				`exchange.declare "events" type="topic" durable=false`,
				`queue.bind "orders" to "events" key="orders.created"`,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			broker := newFakeAMQPBroker(t)

			consumeCmd := findCommand(t, "amqp", "consume")
			resetFlags(t, consumeCmd)
			if err := consumeCmd.ParseFlags(append([]string{"-u", broker.URI()}, tc.args...)); err != nil {
				t.Fatal(err)
			}

			// The whole command would consume until a signal, so only the steps that declare
			// the topology are run.
			if err := amqpCmd.PersistentPreRunE(consumeCmd, nil); err != nil {
				t.Fatalf("creating the subscriber failed: %v", err)
			}
			subscriber := consumer
			t.Cleanup(func() { _ = subscriber.Close() })

			if _, err := subscriber.Subscribe(context.Background(), viper.GetString("amqp.consume.queue")); err != nil {
				t.Fatalf("Subscribe returned error: %v", err)
			}

			assertReceived(t, broker.Received(), tc.want)
		})
	}
}

func TestAMQPProducePublishesStdinToTheExchange(t *testing.T) {
	broker := newFakeAMQPBroker(t)
	stdin := pipeWithInput(t, "hello\n")

	err := runMill(t, stdin, "amqp", "produce", "-u", broker.URI(), "-x", "events", "-r", "orders.created")
	if err != nil {
		t.Fatalf("mill amqp produce returned error: %v", err)
	}

	assertReceived(t, broker.Received(), []string{
		`exchange.declare "events" type="fanout" durable=true`,
		`basic.publish "events" key="orders.created"`,
		`body "hello"`,
	})
}

func runMill(t *testing.T, stdin *os.File, args ...string) error {
	t.Helper()

	resetFlags(t, findCommand(t, args...))

	oldStdin := os.Stdin
	os.Stdin = stdin
	t.Cleanup(func() { os.Stdin = oldStdin })

	// A config file that does not exist keeps a developer's ~/.mill.yaml out of the test.
	rootCmd.SetArgs(append(args, "--config", filepath.Join(t.TempDir(), "absent.yml")))

	done := make(chan error, 1)
	go func() {
		done <- rootCmd.Execute()
	}()

	return waitForProduce(t, done)
}

func findCommand(t *testing.T, args ...string) *cobra.Command {
	t.Helper()

	cmd, _, err := rootCmd.Find(args)
	if err != nil {
		t.Fatal(err)
	}
	return cmd
}

// The flags are package-level, so they keep the values parsed by an earlier test.
func resetFlags(t *testing.T, cmd *cobra.Command) {
	t.Helper()

	for _, flags := range []*pflag.FlagSet{cmd.LocalFlags(), cmd.InheritedFlags()} {
		flags.VisitAll(func(flag *pflag.Flag) {
			if err := flag.Value.Set(flag.DefValue); err != nil {
				t.Fatalf("could not reset flag %s: %v", flag.Name, err)
			}
			flag.Changed = false
		})
	}
}

func assertReceived(t *testing.T, got, want []string) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("broker received:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// fakeAMQPBroker lets the tests run without a real broker. It answers only the synchronous
// methods watermill-amqp waits for.
type fakeAMQPBroker struct {
	listener net.Listener

	lock     sync.Mutex
	received []string
}

func newFakeAMQPBroker(t *testing.T) *fakeAMQPBroker {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	broker := &fakeAMQPBroker{listener: listener}
	go broker.acceptConnections()

	return broker
}

func (b *fakeAMQPBroker) URI() string {
	return "amqp://guest:guest@" + b.listener.Addr().String() + "/"
}

func (b *fakeAMQPBroker) Received() []string {
	b.lock.Lock()
	defer b.lock.Unlock()

	return append([]string(nil), b.received...)
}

func (b *fakeAMQPBroker) record(format string, args ...any) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.received = append(b.received, fmt.Sprintf(format, args...))
}

func (b *fakeAMQPBroker) acceptConnections() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		go b.serve(conn)
	}
}

const (
	amqpFrameMethod = 1
	amqpFrameBody   = 3
)

func (b *fakeAMQPBroker) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	protocolHeader := make([]byte, 8)
	if _, err := io.ReadFull(conn, protocolHeader); err != nil {
		return
	}
	// connection.start: protocol 0-9, no server properties, PLAIN mechanism, en_US locale
	writeAMQPMethod(conn, 0, 10, 10, []byte{0, 9, 0, 0, 0, 0}, amqpLongstr("PLAIN"), amqpLongstr("en_US"))

	for {
		frameType, channel, payload, err := readAMQPFrame(conn)
		if err != nil {
			return
		}
		if frameType == amqpFrameBody {
			b.record("body %q", payload)
			continue
		}
		if frameType != amqpFrameMethod {
			continue // content headers and heartbeats
		}

		args := payload[4:]
		switch method := fmt.Sprintf("%d.%d", binary.BigEndian.Uint16(payload[0:2]), binary.BigEndian.Uint16(payload[2:4])); method {
		case "10.11": // connection.start-ok
			// connection.tune: channel-max 2047, frame-max 128 KiB, no heartbeats
			writeAMQPMethod(conn, 0, 10, 30, []byte{0x07, 0xff, 0, 2, 0, 0, 0, 0})
		case "10.40": // connection.open
			writeAMQPMethod(conn, 0, 10, 41, amqpShortstr(""))
		case "10.50": // connection.close
			// Like RabbitMQ, leave closing the socket to the client. If the server closes it first,
			// amqp091 and watermill-amqp can deadlock in Close (see handleConnectionClose).
			writeAMQPMethod(conn, 0, 10, 51)
		case "20.10": // channel.open
			writeAMQPMethod(conn, channel, 20, 11, amqpLongstr(""))
		case "20.40": // channel.close
			writeAMQPMethod(conn, channel, 20, 41)
		case "40.10": // exchange.declare
			names, flags := amqpMethodShortstrs(args, 2)
			b.record("exchange.declare %q type=%q durable=%t", names[0], names[1], flags&2 != 0)
			writeAMQPMethod(conn, channel, 40, 11)
		case "50.10": // queue.declare
			names, flags := amqpMethodShortstrs(args, 1)
			b.record("queue.declare %q durable=%t", names[0], flags&2 != 0)
			writeAMQPMethod(conn, channel, 50, 11, amqpShortstr(names[0]), []byte{0, 0, 0, 0, 0, 0, 0, 0})
		case "50.20": // queue.bind
			names, _ := amqpMethodShortstrs(args, 3)
			b.record("queue.bind %q to %q key=%q", names[0], names[1], names[2])
			writeAMQPMethod(conn, channel, 50, 21)
		case "60.10": // basic.qos
			writeAMQPMethod(conn, channel, 60, 11)
		case "60.20": // basic.consume
			names, _ := amqpMethodShortstrs(args, 2)
			writeAMQPMethod(conn, channel, 60, 21, amqpShortstr(names[1]))
		case "60.40": // basic.publish
			names, _ := amqpMethodShortstrs(args, 2)
			b.record("basic.publish %q key=%q", names[0], names[1])
		}
	}
}

func readAMQPFrame(r io.Reader) (frameType byte, channel uint16, payload []byte, err error) {
	header := make([]byte, 7)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, 0, nil, err
	}
	size := binary.BigEndian.Uint32(header[3:7])

	payloadAndFrameEnd := make([]byte, size+1)
	if _, err := io.ReadFull(r, payloadAndFrameEnd); err != nil {
		return 0, 0, nil, err
	}

	return header[0], binary.BigEndian.Uint16(header[1:3]), payloadAndFrameEnd[:size], nil
}

func writeAMQPMethod(w io.Writer, channel uint16, class, method uint16, args ...[]byte) {
	payload := binary.BigEndian.AppendUint16(nil, class)
	payload = binary.BigEndian.AppendUint16(payload, method)
	payload = append(payload, bytes.Join(args, nil)...)

	frame := []byte{amqpFrameMethod}
	frame = binary.BigEndian.AppendUint16(frame, channel)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	frame = append(frame, 0xCE) // frame-end

	_, _ = w.Write(frame)
}

// Every method decoded here starts with the deprecated 2-byte ticket field, and its flags octet,
// if any, follows the short strings.
func amqpMethodShortstrs(args []byte, n int) ([]string, byte) {
	args = args[2:]
	strs := make([]string, n)
	for i := range strs {
		size := int(args[0])
		strs[i] = string(args[1 : 1+size])
		args = args[1+size:]
	}
	return strs, args[0]
}

func amqpShortstr(s string) []byte {
	return append([]byte{byte(len(s))}, s...)
}

func amqpLongstr(s string) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(s))), s...)
}
