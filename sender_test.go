package gohub

import (
	"testing"
	"qpid.apache.org/amqp"
)

func TestGetAmqpMessage(t *testing.T) {
	cases := []struct {
		in string
		wantStr string
		inferred bool
		encoded string
	} {
		{`RECOBE`, `RECOBE`, true, `UTF-8`},
	}
	for _, c := range cases {
		got := getAmqpMessage(c.in)
		if string(got.Body().(amqp.Binary)) != c.wantStr ||
			got.Inferred() != c.inferred ||
			got.ContentEncoding() != c.encoded {
			t.Errorf(`getAmqpMessage(%v) == %v, %v, %v <> want %v, %v, %v`,
				c.in, string(got.Body().(amqp.Binary)), got.Inferred(), got.ContentEncoding(),
				c.wantStr, c.inferred, c.encoded)
		}
	}
}