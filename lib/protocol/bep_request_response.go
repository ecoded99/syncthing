// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package protocol

import "github.com/syncthing/syncthing/internal/gen/bep"

type ErrorCode = bep.ErrorCode

const (
	ErrorCodeNoError     = bep.ErrorCode_ERROR_CODE_NO_ERROR
	ErrorCodeGeneric     = bep.ErrorCode_ERROR_CODE_GENERIC
	ErrorCodeNoSuchFile  = bep.ErrorCode_ERROR_CODE_NO_SUCH_FILE
	ErrorCodeInvalidFile = bep.ErrorCode_ERROR_CODE_INVALID_FILE
)

type Request struct {
	// BatchEligible is local only. batchPayload is a negotiated fork extension.
	BatchEligible bool
	batchPayload  []byte
	speedPayload  []byte
	ID            int
	Folder        string
	Name          string
	Offset        int64
	Size          int
	Hash          []byte
	FromTemporary bool
	BlockNo       int
}

func (r *Request) toWire() *bep.Request {
	w := &bep.Request{
		Id:            int32(r.ID),
		Folder:        r.Folder,
		Name:          r.Name,
		Offset:        r.Offset,
		Size:          int32(r.Size),
		Hash:          r.Hash,
		FromTemporary: r.FromTemporary,
		BlockNo:       int32(r.BlockNo),
	}
	if len(r.batchPayload) > 0 {
		setExtension(w, batchRequestField, r.batchPayload)
	}
	if len(r.speedPayload) > 0 {
		setExtension(w, speedRequestField, r.speedPayload)
	}
	return w
}

func requestFromWire(w *bep.Request) *Request {
	return &Request{
		batchPayload:  getExtension(w, batchRequestField),
		speedPayload:  getExtension(w, speedRequestField),
		ID:            int(w.Id),
		Folder:        w.Folder,
		Name:          w.Name,
		Offset:        w.Offset,
		Size:          int(w.Size),
		Hash:          w.Hash,
		FromTemporary: w.FromTemporary,
		BlockNo:       int(w.BlockNo),
	}
}

type Response struct {
	ID   int
	Data []byte
	Code ErrorCode
}

func (r *Response) toWire() *bep.Response {
	return &bep.Response{
		Id:   int32(r.ID),
		Data: r.Data,
		Code: r.Code,
	}
}

func responseFromWire(w *bep.Response) *Response {
	return &Response{
		ID:   int(w.Id),
		Data: w.Data,
		Code: w.Code,
	}
}
