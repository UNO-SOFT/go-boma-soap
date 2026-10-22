// Copyright 2026 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

package boma

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/valyala/bytebufferpool"
	"github.com/valyala/quicktemplate"
)

func TestStreamXMLStress(t *testing.T) {
	t.Parallel()

	sizes := []int{1, 1 << 10, 64 << 10, 1 << 20}
	if !testing.Short() {
		sizes = append(sizes, 8<<20)
	}
	for _, size := range sizes {
		size := size
		t.Run(fmt.Sprintf("SendXmlBomaMsgRequest/%d", size), func(t *testing.T) {
			t.Parallel()
			payload := stressPayload(size)
			data := base64.StdEncoding.EncodeToString([]byte(payload))
			req := newStressSendRequest(size, data)

			doc := streamXMLString(t, req)
			checkWellFormedXML(t, doc)

			var got sendXMLDocument
			if err := xml.Unmarshal([]byte(doc), &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			checkSendDocument(t, got, req)

			decoded, err := base64.StdEncoding.DecodeString(string(got.MsgData))
			if err != nil {
				t.Fatalf("decode msgData: %v", err)
			}
			if string(decoded) != payload {
				t.Errorf("msgData changed: got %d bytes, want %d", len(decoded), len(payload))
			}
			checkWellFormedXML(t, string(decoded))
			validateWithXSD(t, doc)
		})

		t.Run(fmt.Sprintf("extractXmlBomaMsgRequest/%d", size), func(t *testing.T) {
			t.Parallel()
			payload := stressPayload(size)
			data := base64.StdEncoding.EncodeToString([]byte(payload))
			req := extractXmlBomaMsgRequest{
				Type:    "INF_NEW_BM_RATING",
				Key:     "R3lDgWX1taqs7ijZLuzBGvhq00Nl1skNICfSDiHuJZd+WxG72kop6PTLEGrKz0QpsEtuhfgpWY8P6h42G1AQcA==",
				Data:    data,
				Service: "EXTRACT_DEC_BOMA_MSG",
			}

			doc := streamXMLString(t, req)
			checkWellFormedXML(t, doc)

			var got extractXMLDocument
			if err := xml.Unmarshal([]byte(doc), &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got.Type != req.Type || got.Service != req.Service || got.Key != req.Key {
				t.Errorf("header mismatch: got %+v, want %+v", got, req)
			}
			if string(got.Data) != req.Data {
				t.Errorf("msgData changed: got %d bytes, want %d", len(got.Data), len(req.Data))
			}
			decoded, err := base64.StdEncoding.DecodeString(string(got.Data))
			if err != nil {
				t.Fatalf("decode msgData: %v", err)
			}
			if string(decoded) != payload {
				t.Errorf("msgData changed: got %d bytes, want %d", len(decoded), len(payload))
			}
			checkWellFormedXML(t, string(decoded))
			validateWithXSD(t, doc)
		})
	}

	t.Run("SendXmlMultiBomaMsgRequest", func(t *testing.T) {
		t.Parallel()
		for _, n := range []int{1, 8, 64, 512} {
			n := n
			t.Run(fmt.Sprint(n), func(t *testing.T) {
				t.Parallel()
				targets := make([]string, n)
				for i := range targets {
					targets[i] = fmt.Sprintf("%05d", i+1)
				}
				payload := stressPayload(4 << 10)
				req := SendXmlMultiBomaMsgRequest{
					Targets:               targets,
					SendXmlBomaMsgRequest: newStressSendRequest(n, base64.StdEncoding.EncodeToString([]byte(payload))),
				}

				doc := streamXMLString(t, req)
				checkWellFormedXML(t, doc)

				var got multiXMLDocument
				if err := xml.Unmarshal([]byte(doc), &got); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				if !equalStrings(got.TargetList.TargetInsCo, targets) {
					t.Errorf("targets changed: got %v, want %v", got.TargetList.TargetInsCo, targets)
				}
				checkSendDocument(t, got.Request, req.SendXmlBomaMsgRequest)
				if string(got.Request.MsgData) != string(req.Data) {
					t.Errorf("msgData changed: got %d bytes, want %d", len(got.Request.MsgData), len(req.Data))
				}
				validateWithXSD(t, doc)
			})
		}
	})

	t.Run("Escaping", func(t *testing.T) {
		t.Parallel()
		tricky := `a<b>c&d"e'f` + "\u00e9\u4e16\u754c\t\r\n"
		payload := `<root><reasonOfRequest>because &amp; why</reasonOfRequest><text>&lt;tag&gt; &amp; more</text></root>`
		req := SendXmlBomaMsgRequest{
			MsgHeader: MsgHeader{
				Type:   tricky,
				Sender: tricky,
				Target: tricky,
				MsgID:  tricky,
				RefID:  tricky,
				Reason: tricky,
				ReqID:  tricky,
			},
			DateTime: sql.NullTime{Time: time.Date(2026, 10, 22, 12, 34, 56, 0, time.UTC), Valid: true},
			Data:     []byte(payload),
		}

		doc := streamXMLString(t, req)
		checkWellFormedXML(t, doc)

		var got sendXMLDocument
		if err := xml.Unmarshal([]byte(doc), &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		for name, pair := range map[string][2]string{
			"msgTypeName":     {got.Header.Type, req.Type},
			"senderInsCo":     {got.Header.Sender, req.Sender},
			"targetInsCo":     {got.Header.Target, req.Target},
			"messageId":       {got.Header.MsgID, req.MsgID},
			"referenceId":     {got.Header.RefID, req.RefID},
			"reasonOfRequest": {got.Header.Reason, req.Reason},
			"requestersId":    {got.Header.ReqID, req.ReqID},
		} {
			if pair[0] != pair[1] {
				t.Errorf("%s: got %q, want %q", name, pair[0], pair[1])
			}
		}
		if want := req.DateTime.Time.Format(time.RFC3339); got.Header.DateTime != want {
			t.Errorf("messageDateTime: got %q, want %q", got.Header.DateTime, want)
		}
		if string(got.MsgData) != payload {
			t.Errorf("msgData changed: got %q, want %q", got.MsgData, payload)
		}
		checkWellFormedXML(t, string(got.MsgData))

		multi := SendXmlMultiBomaMsgRequest{
			Targets:               []string{tricky, `a&b<c`, `d"e'f`},
			SendXmlBomaMsgRequest: req,
		}
		doc = streamXMLString(t, multi)
		checkWellFormedXML(t, doc)
		var gotMulti multiXMLDocument
		if err := xml.Unmarshal([]byte(doc), &gotMulti); err != nil {
			t.Fatalf("unmarshal multi: %v", err)
		}
		if !equalStrings(gotMulti.TargetList.TargetInsCo, multi.Targets) {
			t.Errorf("targets: got %q, want %q", gotMulti.TargetList.TargetInsCo, multi.Targets)
		}

		extract := extractXmlBomaMsgRequest{
			Type:    "INF_NEW_BM_RATING",
			Key:     tricky,
			Data:    payload,
			Service: "EXTRACT_DEC_BOMA_MSG",
		}
		doc = streamXMLString(t, extract)
		checkWellFormedXML(t, doc)
		var gotExtract extractXMLDocument
		if err := xml.Unmarshal([]byte(doc), &gotExtract); err != nil {
			t.Fatalf("unmarshal extract: %v", err)
		}
		if gotExtract.Key != tricky {
			t.Errorf("encryptedKey: got %q, want %q", gotExtract.Key, tricky)
		}
		if string(gotExtract.Data) != payload {
			t.Errorf("extract msgData changed: got %q, want %q", gotExtract.Data, payload)
		}
	})
}

func newStressSendRequest(size int, data string) SendXmlBomaMsgRequest {
	return SendXmlBomaMsgRequest{
		MsgHeader: MsgHeader{
			Type:   "INF_NEW_BM_RATING",
			Sender: "00001",
			Target: "00002",
			MsgID:  fmt.Sprintf("STRESS-%d", size),
		},
		DateTime: sql.NullTime{Time: time.Date(2026, 10, 22, 12, 34, 56, 0, time.UTC), Valid: true},
		Data:     []byte(data),
	}
}

func streamXMLString(t *testing.T, x interface{ StreamXML(*quicktemplate.Writer) }) string {
	t.Helper()
	B := bytebufferpool.Get()
	defer bytebufferpool.Put(B)
	W := quicktemplate.AcquireWriter(B)
	x.StreamXML(W)
	quicktemplate.ReleaseWriter(W)
	return string(B.Bytes())
}

func stressPayload(size int) string {
	var b strings.Builder
	b.Grow(size + 128)
	b.WriteString(`<payload><items>`)
	for i := 0; b.Len() < size; i++ {
		fmt.Fprintf(&b, `<item id="%d"><name>item %d &amp; &lt;ok&gt;</name><value>%d</value></item>`, i, i, i*31)
	}
	b.WriteString(`</items></payload>`)
	return b.String()
}

func checkWellFormedXML(t *testing.T, doc string) {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(doc))
	for {
		if _, err := dec.Token(); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			t.Fatalf("invalid XML: %v\n%.512s", err, doc)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func checkSendDocument(t *testing.T, got sendXMLDocument, want SendXmlBomaMsgRequest) {
	t.Helper()
	for name, pair := range map[string][2]string{
		"msgTypeName":        {got.Header.Type, want.Type},
		"senderInsCo":        {got.Header.Sender, want.Sender},
		"targetInsCo":        {got.Header.Target, want.Target},
		"messageId":          {got.Header.MsgID, want.MsgID},
		"referenceId":        {got.Header.RefID, want.RefID},
		"reasonOfRequest":    {got.Header.Reason, want.Reason},
		"requestersId":       {got.Header.ReqID, want.ReqID},
		"encryptionRequired": {got.EncryptionRequired, "true"},
		"serviceRequested":   {got.ServiceRequested, "SEND"},
		"messageDateTime":    {got.Header.DateTime, want.DateTime.Time.Format(time.RFC3339)},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s: got %q, want %q", name, pair[0], pair[1])
		}
	}
	if !bytes.Equal(got.MsgData, want.Data) {
		t.Errorf("msgData changed: got %d bytes, want %d", len(got.MsgData), len(want.Data))
	}
}

var xmllint = sync.OnceValue(func() string {
	s, _ := exec.LookPath("xmllint")
	return s
})

const stressSchema = "soapXmlMsgServices.xsd"

func validateWithXSD(t *testing.T, doc string) {
	t.Helper()
	schema := filepath.Join("testdata", "xsd", stressSchema)
	if _, err := os.Stat(schema); err != nil {
		t.Logf("skipping XSD validation: %v", err)
		return
	}
	if xmllint() == "" {
		t.Logf("skipping XSD validation: xmllint is not found")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, xmllint(), "--noout", "--stream", "--schema", stressSchema, "-")
	cmd.Dir = filepath.Dir(schema)
	cmd.Stdin = strings.NewReader(doc)
	var errBuf strings.Builder
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		t.Errorf("%s: %v: %s", stressSchema, err, errBuf.String())
	}
}

type sendXMLDocument struct {
	Header struct {
		Type     string `xml:"msgTypeName"`
		Sender   string `xml:"senderInsCo"`
		Target   string `xml:"targetInsCo"`
		MsgID    string `xml:"messageId"`
		RefID    string `xml:"referenceId"`
		Reason   string `xml:"reasonOfRequest"`
		DateTime string `xml:"messageDateTime"`
		ReqID    string `xml:"requestersId"`
	} `xml:"msgHeader"`
	EncryptionRequired string `xml:"encryptionRequired"`
	ServiceRequested   string `xml:"serviceRequested"`
	MsgData            []byte `xml:"msgData"`
}

type multiXMLDocument struct {
	TargetList struct {
		TargetInsCo []string `xml:"targetInsCo"`
	} `xml:"targetList"`
	Request sendXMLDocument `xml:"sendXmlBomaMsgRequest"`
}

type extractXMLDocument struct {
	Type    string `xml:"msgTypeName"`
	Service string `xml:"serviceRequested"`
	Key     string `xml:"encryptedKey"`
	Data    []byte `xml:"msgData"`
}
