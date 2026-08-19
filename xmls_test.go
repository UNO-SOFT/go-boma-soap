// Copyright 2017, 2026 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package boma

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestExtractXml(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	got := extractXmlBomaMsgRequest{
		Type:    "INF_NEW_BM_RATING",
		Key:     `R3lDgWX1taqs7ijZLuzBGvhq00Nl1skNICfSDiHuJZd+WxG72kop6PTLEGrKz0QpsEtuhfgpWY8P6h42G1AQcA==`,
		Data:    `ZqDX0DD4PCGF4SPfIj9h2sxyGBwPlgqJ4OaYyv+VoN3F6kNdOC9m11+MrQUiKTnd9pK20G6sR8vGaSZ3fuKkvyYai6NXr1wotTHwNhGoaADXO4w+5y6bdM8zgWtqogp0OP0Rfu82XVSpyf2w5DCfIc/T68wfHZffqNkLrkgJPcRUcBzIAkuIEchBjLKb8nP/5P8LeY6FLG5DtL0gKZ6U0658CUyAwrVT41dXT2IEz8aOsf2q3mBjBEzUs1LZVGc9mFduR85clMdlAAIlOjDBI++Wkc7+mnTg8Bpv1JkuCQYeutSAhMkNHPlmKLhcVa4X4/W+ib9D095cM+4V5kfGBdIynXtdxuLftGj5OdvLPhi6pBvIGjKxyRgRTANL9AHj8LGA/c9tnqyJC4+hmzWmez31dZy4KnuKE93jMnVim1pjjaTboXtJtv4AMY3USh/W9sFX3l+Yocb1+vx87/MNk84Qb6wMoF3UZZUvmj+f/HL6FP4EihJWKuZBMion1BLBFlK13fOmM1v7oPaHTtfpgfe6PZRmwdbfgaJyNFjAUfqAO0ktg+2deZ96trFzxVFkI5V9VdoI6YqseSSXQFBvBkyzwFO4DTd7Yzpp6VU2jOSBpKJyLXccM6Wy+CeiSruld9kJfyrDdkFvBfsWAmADcl6WpNT1K/Xkv7R5pR43L69B/KAnyF0Cx1vNbbpraBMI`,
		Service: "EXTRACT_DEC_BOMA_MSG",
	}.XML()

	t.Log(got)

	var errBuf strings.Builder
	schema := "soapXmlMsgServices.xsd"
	cmd := exec.CommandContext(ctx, "xmllint", "--noout", "--schema", schema, "-")
	cmd.Dir = "../xsd"
	cmd.Stdin = strings.NewReader(got)
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		t.Errorf("%s: %s", schema, errBuf.String())
	}
}
