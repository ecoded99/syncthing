// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package archivebatch

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
)

const maxExtraBytes = 256

// Validate central and local inventory before archive/zip allocates its file
// table. The wire subset deliberately excludes ZIP64, archive/file comments,
// multi-disk files, prefixes, gaps, encryption and unreferenced local entries.
// Pack emits this subset. Sizes/offsets are <=MaxArchiveBytes, so all arithmetic
// below is bounded well below int64 overflow. No member data is expanded here.
func validateZIP(r io.ReaderAt, size int64, expected map[string]int) error {
	bad := func(reason string) error { return fmt.Errorf("archivebatch: invalid ZIP inventory: %s", reason) }
	if size < 22 || size > MaxArchiveBytes {
		return bad("archive size")
	}
	var end [22]byte
	if _, err := r.ReadAt(end[:], size-22); err != nil {
		return err
	}
	if u32(end[:4]) != 0x06054b50 || u16(end[4:6]) != 0 || u16(end[6:8]) != 0 || u16(end[20:22]) != 0 {
		return bad("end record, disk or comment")
	}
	count := int(u16(end[10:12]))
	centralSize, centralOffset := int64(u32(end[12:16])), int64(u32(end[16:20]))
	if count != len(expected) || count < 1 || count > MaxFiles || u16(end[8:10]) != uint16(count) || centralSize > MaxFiles*(46+MaxNameBytes+maxExtraBytes) || centralOffset+centralSize != size-22 {
		return bad("central count or bounds")
	}
	type span struct{ start, end int64 }
	spans := make([]span, 0, count)
	seen := make(map[string]bool, count)
	offset := centralOffset
	for range count {
		if offset+46 > size-22 {
			return bad("central header truncated")
		}
		var header [46]byte
		if _, err := r.ReadAt(header[:], offset); err != nil {
			return err
		}
		if u32(header[:4]) != 0x02014b50 || u16(header[34:36]) != 0 {
			return bad("central signature or disk")
		}
		flags, method := u16(header[8:10]), u16(header[10:12])
		compressed, expanded := int64(u32(header[20:24])), int64(u32(header[24:28]))
		nameLen, extraLen, commentLen := int(u16(header[28:30])), int(u16(header[30:32])), int(u16(header[32:34]))
		localOffset := int64(u32(header[42:46]))
		allowedFlags := uint16(0x808) // UTF8 and data descriptor.
		if method == 8 {
			allowedFlags |= 6
		} // Deflate level hints.
		if method != 0 && method != 8 || flags & ^allowedFlags != 0 || nameLen < 1 || nameLen > MaxNameBytes || extraLen > maxExtraBytes || commentLen != 0 || expanded > MaxFileBytes || compressed > MaxArchiveBytes || method == 0 && compressed != expanded {
			return bad("central method, flags, size or metadata")
		}
		offset += 46
		if offset+int64(nameLen+extraLen) > size-22 {
			return bad("central variable fields")
		}
		fields := make([]byte, nameLen+extraLen)
		if _, err := r.ReadAt(fields, offset); err != nil {
			return err
		}
		name := string(fields[:nameLen])
		if _, ok := expected[name]; !ok || seen[name] {
			return bad("unexpected or duplicate member")
		}
		if err := validName(name); err != nil {
			return err
		}
		if err := validExtra(fields[nameLen:]); err != nil {
			return err
		}
		seen[name] = true
		offset += int64(len(fields))
		if localOffset+30 > centralOffset {
			return bad("local header outside data region")
		}
		var local [30]byte
		if _, err := r.ReadAt(local[:], localOffset); err != nil {
			return err
		}
		localName, localExtra := int(u16(local[26:28])), int(u16(local[28:30]))
		if u32(local[:4]) != 0x04034b50 || u16(local[6:8]) != flags || u16(local[8:10]) != method || localName != nameLen || localExtra > maxExtraBytes {
			return bad("local header differs")
		}
		dataOffset := localOffset + 30 + int64(localName+localExtra)
		dataEnd := dataOffset + compressed
		if dataOffset > centralOffset || dataEnd > centralOffset {
			return bad("entry data outside archive")
		}
		localFields := make([]byte, localName+localExtra)
		if _, err := r.ReadAt(localFields, localOffset+30); err != nil {
			return err
		}
		if string(localFields[:localName]) != name {
			return bad("local and central names differ")
		}
		if err := validExtra(localFields[localName:]); err != nil {
			return err
		}
		crc := u32(header[16:20])
		if flags&8 == 0 {
			if u32(local[14:18]) != crc || int64(u32(local[18:22])) != compressed || int64(u32(local[22:26])) != expanded {
				return bad("local sizes or checksum differ")
			}
		} else {
			if dataEnd+16 > centralOffset {
				return bad("descriptor outside data region")
			}
			var descriptor [16]byte
			if _, err := r.ReadAt(descriptor[:], dataEnd); err != nil {
				return err
			}
			if u32(descriptor[:4]) != 0x08074b50 || u32(descriptor[4:8]) != crc || int64(u32(descriptor[8:12])) != compressed || int64(u32(descriptor[12:16])) != expanded {
				return bad("descriptor differs")
			}
			dataEnd += 16
		}
		spans = append(spans, span{localOffset, dataEnd})
	}
	if offset != size-22 {
		return bad("extra central entries or trailing metadata")
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
	var cursor int64
	for _, span := range spans {
		if span.start != cursor || span.end <= span.start {
			return bad("overlapping, hidden or gapped local entries")
		}
		cursor = span.end
	}
	if cursor != centralOffset {
		return bad("unlisted data before central directory")
	}
	return nil
}

func validExtra(extra []byte) error {
	for len(extra) > 0 {
		if len(extra) < 4 {
			return errors.New("archivebatch: malformed ZIP extra")
		}
		id, size := u16(extra[:2]), int(u16(extra[2:4]))
		if id == 1 || size > len(extra)-4 {
			return errors.New("archivebatch: ZIP64 or malformed ZIP extra")
		}
		extra = extra[4+size:]
	}
	return nil
}
func u16(b []byte) uint16 { return binary.LittleEndian.Uint16(b) }
func u32(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }
