// Copyright 2017, 2026 Tamás Gulácsi. All rights reserved.

package soap

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/UNO-SOFT/zlog/v2"
	"github.com/sony/gobreaker/v2"
	"github.com/tgulacsi/go/bufpool"
	"github.com/tgulacsi/go/soaphlp"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/time/rate"
)

var (
	DefaultRateLimit = rate.Limit(3)

	ErrSend         = errors.New("send")
	ErrLocked       = errors.New("locked")
	ErrEmptyMessage = errors.New("empty message")

	restartLimiter = rate.NewLimiter(1.0/3600.0, 1)
)

type SendReceiver interface {
	Sender
	Receiver
}
type Sender interface {
	Send(context.Context, io.Reader) error
}
type Receiver interface {
	Receive(ctx context.Context, w io.Writer) (ReceiveRootMsgResponse, error)
	Decrypter
}
type Decrypter interface {
	Decrypt(ctx context.Context, w io.Writer, key, msgTypeName string, data []byte) error
	DecryptReader(ctx context.Context, w io.Writer, r io.Reader) (MsgHeader, error)
}

type soapClient struct {
	Caller  soaphlp.Caller
	limiter *rate.Limiter
	cb      *gobreaker.CircuitBreaker[*xml.Decoder]
}

func NewSOAP(endpointURL string, client *http.Client, rateLimit *rate.Limiter) soapClient {
	if client == nil {
		client = http.DefaultClient
	}
	if rateLimit == nil {
		rateLimit = rate.NewLimiter(DefaultRateLimit, 1)
	}
	// zlog.SFromContext(ctx).Info("NewSOAP", "url", endpointURL, "limit", rateLimit.Limit())
	return soapClient{
		Caller:  soaphlp.NewClient(endpointURL, "", client),
		limiter: rateLimit,
		cb: gobreaker.NewCircuitBreaker[*xml.Decoder](gobreaker.Settings{
			Name: "boma-soap",
			// MaxRequests is the maximum number of requests allowed to pass through when the CircuitBreaker is half-open. If MaxRequests is 0, the CircuitBreaker allows only 1 request.
			MaxRequests: 1,

			// Interval is the cyclic period of the closed state for the CircuitBreaker to clear the internal Counts. If Interval is less than or equal to 0, the CircuitBreaker does not clear internal Counts during the closed state.
			Interval: 10 * time.Minute,

			// BucketPeriod defines the time duration for each bucket in the rolling window strategy. The internal Counts will be updated and reset gradually for each bucket. Interval will be automatically adjusted to be a multiple of BucketPeriod. If BucketPeriod is less than or equal to 0, the CircuitBreaker will use a fixed window strategy instead.
			BucketPeriod: time.Minute,

			// Timeout is the period of the open state, after which the state of the CircuitBreaker becomes half-open. If Timeout is less than or equal to 0, the timeout value of the CircuitBreaker is set to 60 seconds.
			Timeout: time.Minute,

			// ReadyToTrip is called with a copy of Counts whenever a request fails in the closed state. If ReadyToTrip returns true, the CircuitBreaker will be placed into the open state. If ReadyToTrip is nil, default ReadyToTrip is used. Default ReadyToTrip returns true when the number of consecutive failures is more than 5.

			// OnStateChange is called whenever the state of the CircuitBreaker changes.
			OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
				slog.Warn("circuit-break state change", "from", from, "to", to, "name", name)
				if to != gobreaker.StateOpen {
					return
				}
				reserve := restartLimiter.Reserve()
				if !reserve.OK() || reserve.Delay() > 10*time.Second {
					reserve.Cancel()
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				slog.Warn("restart boma-soap")
				b, err := exec.CommandContext(ctx, "br_ctr", "restart", "boma-soap").CombinedOutput()
				if err != nil {
					slog.Error("br_ctr restart boma-soap", "error", err, "out", string(b))
				} else {
					slog.Warn("br_ctr restart boma-soap", "out", string(b))
				}
			},

			// IsSuccessful is called with the error returned from a request. If IsSuccessful returns true, the error is counted as a success. Otherwise the error is counted as a failure. If IsSuccessful is nil, default IsSuccessful is used, which returns false for all non-nil errors.
			IsSuccessful: func(err error) bool {
				if err == nil || errors.Is(err, io.EOF) {
					return true
				}
				hse, ok := errors.AsType[*soaphlp.HTTPStatusError](err)
				return !ok || hse.StatusCode < 400
			},

			// IsExcluded determines whether a request error should be ignored for the purposes of updating the circuit breaker metrics. If IsExcluded returns true for a given error, the request is neither counted as a success nor as a failure. This can be used, for example, to ignore context cancellations or other errors that should not affect the circuit breaker state. If IsExcluded is nil, no requests are excluded.
			IsExcluded: func(err error) bool { return errors.Is(err, context.Canceled) },
		}),
	}
}

func (cl soapClient) call(ctx context.Context, w io.Writer, r io.Reader) (*xml.Decoder, error) {
	if err := cl.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return cl.cb.Execute(func() (*xml.Decoder, error) {
		return cl.Caller.Call(ctx, w, "", r)
	})
}

func (cl soapClient) Receive(ctx context.Context, w io.Writer) (ReceiveRootMsgResponse, error) {
	if err := ctx.Err(); err != nil {
		return ReceiveRootMsgResponse{}, err
	}
	const receiveRootMsgRequest = `<receiveRootMsgRequest xmlns="http://www.qualysoft.hu/services/schemas/boma/rootmsgservices" xmlns:common="http://www.qualysoft.hu/services/schemas/boma/common" xmlns:basebns="http://www.mabisz.hu/boma">
  <serviceRequested>RECEIVE</serviceRequested>
</receiveRootMsgRequest>
`
	buf := bufpool.Get()
	defer bufpool.Put(buf)
	d, err := cl.call(ctx, io.MultiWriter(w, buf), strings.NewReader(receiveRootMsgRequest))
	if err != nil {
		return ReceiveRootMsgResponse{}, fmt.Errorf("call %s: %s: %w", receiveRootMsgRequest, buf.String(), err)
	}
	var v ReceiveRootMsgResponse
	if err = d.Decode(&v); err != nil {
		return v, fmt.Errorf("ReceiveRootMsgResponse.Decode %s: %w", buf.String(), err)
	}
	v.Header.Responded = v.Responded
	return v, nil
}

// betteralign:ignore
type extractXmlBomaMsgRequest struct {
	Type    string
	Key     string
	Data    string
	Service string
}

func (cl soapClient) Decrypt(ctx context.Context, w io.Writer, key, msgTypeName string, msg []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(msg) == 0 {
		return errors.New("cannot decrypt empty message")
	}
	B := bytebufferpool.Get()
	defer bytebufferpool.Put(B)
	req := extractXmlBomaMsgRequest{
		Type: msgTypeName, Key: key, Data: string(msg),
		Service: "EXTRACT_DEC_BOMA_MSG",
	}
	req.WriteXML(B)
	logger := zlog.SFromContext(ctx)
	if logger.Enabled(ctx, slog.LevelDebug) {
		logger.Debug("Decrypt", "Data", req.Data)
	}
	d, err := cl.call(ctx, io.Discard, bytes.NewReader(B.Bytes()))
	if err != nil {
		logger.Error("Decrypt", "Data", req.Data, "error", err)
		return fmt.Errorf("Decrypt %s: %w", B.String(), err)
	}
	var v ExtractXmlBomaMsgResponse
	err = d.Decode(&v)
	if _, err1 := w.Write(v.Data); err == nil {
		err = err1
	}
	return err
}

type ExtractXmlBomaMsgResponse struct {
	Data []byte `xml:"msgData"`
}

func (cl soapClient) DecryptReader(ctx context.Context, w io.Writer, r io.Reader) (MsgHeader, error) {
	buf := bufpool.Get()
	defer bufpool.Put(buf)
	var v ReceiveRootMsgResponse
	logger := zlog.SFromContext(ctx)
	dec := xml.NewDecoder(io.TeeReader(r, buf))
	for {
		tok, err := dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return MsgHeader{}, err
		}
		if st, ok := tok.(xml.StartElement); ok && st.Name.Local == "receiveRootMsgResponse" {
			logger.Debug("startElement", "st", st)
			var v2 struct {
				Header MsgHeader `xml:"msgHeader"`
				Data   struct {
					Data []byte `xml:",chardata"`
				} `xml:"msgData"`
				Responded string `xml:"serviceResponded"`
			}
			if err := dec.DecodeElement(&v2, &st); err == nil {
				logger.Debug("v2", "v2", v2)
				v.Header, v.Responded = v2.Header, v2.Responded
				if v.Data, err = base64.StdEncoding.AppendDecode(v.Data[:0], v2.Data.Data); err != nil {
					v.Data = v2.Data.Data
				}
			} else {
				var v3 struct {
					Header MsgHeader `xml:"msgHeader"`
					Data   struct {
						Data []byte `xml:",innerxml"`
					} `xml:"msgData"`
					Responded string `xml:"serviceResponded"`
				}
				if err := dec.DecodeElement(&v3, &st); err != nil {
					return MsgHeader{}, fmt.Errorf("decode as receiveRootMsgResponse: %s: %w", buf.String(), err)
				}
				logger.Debug("v3", "v3", v3)
				v.Header, v.Responded = v3.Header, v3.Responded
				if v.Data, err = base64.StdEncoding.AppendDecode(v.Data[:0], v3.Data.Data); err != nil {
					v.Data = v3.Data.Data
				}
			}
			logger.Debug("v", "v", v)
			break
		}
	}
	logger.Info("Decode", "type", v.Header.Type, "responded", v.Responded, "data", string(v.Data), "raw", buf.Len())
	if len(v.Data) == 0 {
		logger.Error("empty Data", "v", v)
		return v.Header, fmt.Errorf("empty Data from\n%s", buf.String())
	}
	return v.Header, cl.Decrypt(ctx, w, v.Header.Key, v.Header.Type, v.Data)
}

type ReceiveRootMsgResponse struct {
	// betteralign:ignore
	Header    MsgHeader `xml:"msgHeader"`
	Data      []byte    `xml:"msgData"`
	Responded string    `xml:"serviceResponded"`
}

func (v ReceiveRootMsgResponse) Decrypt(ctx context.Context, w io.Writer, cl Decrypter) error {
	zlog.SFromContext(ctx).Info("Decrypt", "received", len(v.Data), "type", v.Header.Type, "responded", len(v.Responded))
	if v.Responded == "" || len(v.Data) == 0 {
		return fmt.Errorf("Decrypt empty %+v: %w", v, ErrEmptyMessage)
	}
	if v.Responded != "ROOT_MSG" {
		return fmt.Errorf("got %s", v.Responded)
	}

	return cl.Decrypt(ctx, w, v.Header.Key, v.Header.Type, v.Data)
}

type SendXmlBomaMsgResponse struct {
	Header    MsgHeader `xml:"msgHeader"`
	Responded string    `xml:"serviceResponded"`
	Key       string    `xml:"encryptedKey"`
	Data      []byte    `xml:"msgData"`
}
type MsgHeader struct {
	Type      string `xml:"msgTypeName"`
	Sender    string `xml:"senderInsCo"`
	Target    string `xml:"targetInsCo"`
	MsgID     string `xml:"messageId"`
	RefID     string `xml:"referenceId"`
	Reason    string `xml:"reasonOfRequest"`
	Result    string `xml:"resultCode"`
	DateTime  string `xml:"messageDateTime"`
	ReqID     string `xml:"requestersId"`
	Key       string `xml:"encryptedKey"`
	Responded string `xml:"-"`
}

func (cl soapClient) Send(ctx context.Context, r io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	buf := bufpool.Get()
	defer bufpool.Put(buf)
	d, err := cl.call(ctx, buf, r)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return fmt.Errorf("%s: %w", buf.String(), err)
	}
	logger := zlog.SFromContext(ctx)
	var v SendXmlBomaMsgResponse
	if err := d.Decode(&v); err != nil {
		logger.Error("Decode", "d", d, "error", err)
		return fmt.Errorf("%+v: %w", v, err)
	}
	if v.Responded != "SENT_OK" {
		logger.Error("Send", "responnded", v.Responded)
		return fmt.Errorf("%s: %w", v.Responded, ErrSend)
	}
	return nil
}
