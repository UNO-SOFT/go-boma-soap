// Copyright 2017, 2026 Tamás Gulácsi. All rights reserved.
//
// SDPX-License-Identifier: AGPL-3.0

package boma

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/UNO-SOFT/zlog/v2"
)

const defaultMsgSize = 1 << 20

var bpool = sync.Pool{New: func() any { var buf bytes.Buffer; return &buf }}
var spool = sync.Pool{New: func() any { var buf strings.Builder; return &buf }}

// ReceiveOne receives and decrypts one message from the vault or
// if that's empty, de client.
func ReceiveOne(ctx context.Context,
	vault *Vault, cl Receiver,
	process func(context.Context, *MsgHeader, []byte) error,
) error {
	logger := zlog.SFromContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	buf := bpool.Get().(*bytes.Buffer)
	defer bpool.Put(buf)
	buf.Reset()

	rd, vaultErr := vault.Get()
	if vaultErr != nil {
		if !errors.Is(vaultErr, io.EOF) {

			return vaultErr
		}
	} else {
		logger := logger.With("file", rd.Name())

		raw := spool.Get().(*strings.Builder)
		defer spool.Put(raw)
		raw.Reset()

		hdr, err := cl.DecryptReader(ctx, buf, io.TeeReader(rd, raw))
		rd.Close()
		logger = logger.With("msgID", hdr.MsgID, "type", hdr.Type, "sender", hdr.Sender, "refID", hdr.RefID, "reqID", hdr.ReqID)
		if err != nil {
			if !errors.Is(err, ErrEmptyMessage) {
				logger.Error("vault DecryptReader", "data", raw.String(), "error", err)
				return fmt.Errorf("%s: %w", raw.String(), err)
			}
			logger.Warn("vault DecryptReader", "data", raw.String(), "error", err)
		} else if err = process(ctx, &hdr, buf.Bytes()); err != nil {
			logger.Error("process message", "error", err)
			return err
		}
		rd.Delete()
	}

	w, err := vault.NewWriter(defaultMsgSize)
	if err != nil {
		return fmt.Errorf("allocate %d bytes: %w", defaultMsgSize, err)
	}
	// Save the response byte-for-byte into w (vault)
	msg, err := cl.Receive(ctx, w)
	logger = logger.With("msgID", msg.Header.MsgID, "type", msg.Header.Type, "sender", msg.Header.Sender, "refID", msg.Header.RefID, "reqID", msg.Header.ReqID)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("%s: %w", err, ErrEmptyMessage)
		}
		_ = w.CloseWithError(err)
		if errors.Is(err, ErrEmptyMessage) {
			// empty response
			logger.Debug("empty message", "error", err)
			return nil
		}
		logger.Error("Receive", "error", err)
		return fmt.Errorf("receive: %w", err)
	}

	buf.Reset()
	if err = msg.Decrypt(ctx, buf, cl); err != nil || buf.Len() == 0 {
		if err == nil && buf.Len() == 0 {
			err = ErrEmptyMessage
		}
		_ = w.CloseWithError(err)
		if errors.Is(err, ErrEmptyMessage) {
			logger.Warn("Decrypt empty", "msg", msg)
			return nil
		}
		logger.Error("Decrypt", "msg", msg, "error", err)
		return fmt.Errorf("%+v.Decrypt: %w", msg, err)
	}

	// Save as we could decrypt it
	if err = w.CloseWithError(nil); err != nil {
		logger.Error("Receive", "error", err)
		return err
	}

	if err = process(ctx, &msg.Header, buf.Bytes()); err != nil {
		logger.Error("process message", "error", err)
		return err
	}
	vault.Remove(w.Name())
	return nil
}
