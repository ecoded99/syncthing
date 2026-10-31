// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package cloudbackup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2"
)

const (
	folderMime  = "application/vnd.google-apps.folder"
	driveAPI    = "https://www.googleapis.com/drive/v3"
	driveFields = "id,name,mimeType,parents,size,md5Checksum,modifiedTime"
)

// Smaller files use one multipart request; larger ones a resumable session.
// Chunks must be multiples of 256 KiB. Variables so tests can shrink them.
var (
	resumableThreshold int64 = 5 << 20
	resumableChunk     int   = 16 << 20
)

// driveFile is the subset of Drive file metadata the backup relies on.
type driveFile struct {
	ID            string            `json:"id,omitempty"`
	Name          string            `json:"name,omitempty"`
	MimeType      string            `json:"mimeType,omitempty"`
	Parents       []string          `json:"parents,omitempty"`
	Size          string            `json:"size,omitempty"`
	MD5           string            `json:"md5Checksum,omitempty"`
	ModifiedTime  string            `json:"modifiedTime,omitempty"`
	AppProperties map[string]string `json:"appProperties,omitempty"`
}

func (f driveFile) isFolder() bool { return f.MimeType == folderMime }
func (f driveFile) size() int64 {
	n, _ := strconv.ParseInt(f.Size, 10, 64)
	return n
}
func (f driveFile) modified() time.Time {
	t, _ := time.Parse(time.RFC3339Nano, f.ModifiedTime)
	return t
}

// driveTime formats a modification time at the millisecond precision Drive keeps.
func driveTime(t time.Time) string {
	return t.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z")
}

// drive is a minimal Drive v3 client. The caller supplies an authorized HTTP
// client; with the drive.file scope only files this app created are visible.
type drive struct {
	client    *http.Client
	api       string
	uploadAPI string
}

type driveError struct {
	Status int
	Body   string
}

func (e *driveError) Error() string {
	return fmt.Sprintf("google drive: HTTP %d: %s", e.Status, strings.TrimSpace(e.Body))
}

// retryable reports rate limits and server errors, which Drive asks clients to
// retry with backoff.
func retryable(err error) bool {
	var de *driveError
	if errors.As(err, &de) {
		return de.Status == http.StatusTooManyRequests || de.Status >= 500 ||
			de.Status == http.StatusForbidden && (strings.Contains(de.Body, "rateLimitExceeded") || strings.Contains(de.Body, "userRateLimitExceeded"))
	}
	// Dropped connections and timeouts are transient. Cancellation, yielding
	// to primary sync and authorization failures are not.
	var ue *url.Error
	var re *oauth2.RetrieveError
	return errors.As(err, &ue) && !errors.As(err, &re) && !errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, errPrimaryBusy)
}

// Bounds of the run-wide upload backoff. Variables so tests can shrink them.
var (
	paceMin = time.Second
	paceMax = 64 * time.Second
)

// uploadAttempts bounds retries of one file before it is reported and left
// for the next run.
const uploadAttempts = 8

// pacer slows the whole run while Drive reports rate limits or server errors,
// so the following files wait too instead of failing one after another.
type pacer struct {
	delay time.Duration
}

func (p *pacer) wait(ctx context.Context) error {
	if p.delay == 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(p.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p *pacer) failed() { p.delay = min(max(p.delay*2, paceMin), paceMax) }

// succeeded recovers gradually, so a sustained limit is not hit again at once.
func (p *pacer) succeeded() {
	p.delay /= 2
	if p.delay < paceMin {
		p.delay = 0
	}
}

func withRetry(ctx context.Context, fn func() error) error {
	delay := time.Second
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil || attempt == 4 || !retryable(err) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay *= 2
	}
}

func (d *drive) do(req *http.Request, out any) error {
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &driveError{resp.StatusCode, string(body)}
	}
	if out == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return err
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (d *drive) jsonRequest(ctx context.Context, method, url string, body, out any) error {
	return withRetry(ctx, func() error {
		var r io.Reader
		if body != nil {
			data, err := json.Marshal(body)
			if err != nil {
				return err
			}
			r = bytes.NewReader(data)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, r)
		if err != nil {
			return err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json; charset=UTF-8")
		}
		return d.do(req, out)
	})
}

type driveAbout struct {
	User struct {
		EmailAddress string `json:"emailAddress"`
	} `json:"user"`
	StorageQuota struct {
		Limit string `json:"limit"`
		Usage string `json:"usage"`
	} `json:"storageQuota"`
}

// available returns the remaining quota, or -1 for unlimited accounts.
func (a driveAbout) available() int64 {
	if a.StorageQuota.Limit == "" {
		return -1
	}
	limit, _ := strconv.ParseInt(a.StorageQuota.Limit, 10, 64)
	usage, _ := strconv.ParseInt(a.StorageQuota.Usage, 10, 64)
	return limit - usage
}

func (d *drive) about(ctx context.Context) (driveAbout, error) {
	var a driveAbout
	err := d.jsonRequest(ctx, http.MethodGet, d.api+"/about?fields=user(emailAddress),storageQuota(limit,usage)", nil, &a)
	return a, err
}

// list returns every nontrashed file matching q, following pagination.
func (d *drive) list(ctx context.Context, q string) ([]driveFile, error) {
	var files []driveFile
	page := ""
	for {
		v := url.Values{}
		v.Set("q", q)
		v.Set("pageSize", "1000")
		v.Set("spaces", "drive")
		v.Set("fields", "nextPageToken,files("+driveFields+",appProperties)")
		if page != "" {
			v.Set("pageToken", page)
		}
		var resp struct {
			NextPageToken string      `json:"nextPageToken"`
			Files         []driveFile `json:"files"`
		}
		if err := d.jsonRequest(ctx, http.MethodGet, d.api+"/files?"+v.Encode(), nil, &resp); err != nil {
			return nil, err
		}
		files = append(files, resp.Files...)
		if resp.NextPageToken == "" {
			return files, nil
		}
		page = resp.NextPageToken
	}
}

func (d *drive) createFolder(ctx context.Context, name, parent string, props map[string]string) (driveFile, error) {
	meta := driveFile{Name: name, MimeType: folderMime, AppProperties: props}
	if parent != "" {
		meta.Parents = []string{parent}
	}
	var f driveFile
	err := d.jsonRequest(ctx, http.MethodPost, d.api+"/files?fields="+driveFields, meta, &f)
	return f, err
}

func (d *drive) copy(ctx context.Context, id, name, parent string) error {
	return d.jsonRequest(ctx, http.MethodPost, d.api+"/files/"+url.PathEscape(id)+"/copy?fields=id", driveFile{Name: name, Parents: []string{parent}}, nil)
}

// upload creates a file (id empty) or replaces its content. The body is read
// exactly once; callers retry by reopening the source.
func (d *drive) upload(ctx context.Context, id string, meta driveFile, body io.Reader, size int64) (driveFile, error) {
	method, target := http.MethodPost, d.uploadAPI+"/files"
	if id != "" {
		method, target = http.MethodPatch, target+"/"+url.PathEscape(id)
		// Parents cannot be set on update; the file keeps its location.
		meta.Parents = nil
	}
	target += "?fields=" + driveFields
	if size >= resumableThreshold {
		return d.resumable(ctx, method, target, meta, body, size)
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return driveFile{}, err
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	part, _ := mw.CreatePart(textproto.MIMEHeader{"Content-Type": {"application/json; charset=UTF-8"}})
	part.Write(metaJSON)
	part, _ = mw.CreatePart(textproto.MIMEHeader{"Content-Type": {"application/octet-stream"}})
	if _, err := io.Copy(part, body); err != nil {
		return driveFile{}, err
	}
	mw.Close()
	req, err := http.NewRequestWithContext(ctx, method, target+"&uploadType=multipart", &buf)
	if err != nil {
		return driveFile{}, err
	}
	req.Header.Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
	var f driveFile
	return f, d.do(req, &f)
}

func (d *drive) resumable(ctx context.Context, method, target string, meta driveFile, body io.Reader, size int64) (driveFile, error) {
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return driveFile{}, err
	}
	req, err := http.NewRequestWithContext(ctx, method, target+"&uploadType=resumable", bytes.NewReader(metaJSON))
	if err != nil {
		return driveFile{}, err
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("X-Upload-Content-Type", "application/octet-stream")
	req.Header.Set("X-Upload-Content-Length", strconv.FormatInt(size, 10))
	resp, err := d.client.Do(req)
	if err != nil {
		return driveFile{}, err
	}
	resp.Body.Close()
	session := resp.Header.Get("Location")
	if resp.StatusCode/100 != 2 || session == "" {
		return driveFile{}, &driveError{resp.StatusCode, "resumable session not started"}
	}
	chunk := make([]byte, resumableChunk)
	var offset int64
	for {
		n, err := io.ReadFull(body, chunk)
		if err != nil && err != io.ErrUnexpectedEOF && !(err == io.EOF && offset == size) {
			return driveFile{}, err
		}
		end := offset + int64(n)
		if end > size || n < len(chunk) && end != size {
			return driveFile{}, errors.New("source size changed during upload")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, session, bytes.NewReader(chunk[:n]))
		if err != nil {
			return driveFile{}, err
		}
		req.ContentLength = int64(n)
		if n > 0 {
			req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, end-1, size))
		} else {
			req.Header.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		}
		resp, err := d.client.Do(req)
		if err != nil {
			return driveFile{}, err
		}
		if end < size {
			resp.Body.Close()
			// 308 Resume Incomplete acknowledges a chunk. Chunks are multiples of
			// 256 KiB, so Drive stores each completely or reports an error.
			if resp.StatusCode != http.StatusPermanentRedirect {
				return driveFile{}, &driveError{resp.StatusCode, "chunk rejected"}
			}
			offset = end
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return driveFile{}, &driveError{resp.StatusCode, string(b)}
		}
		var f driveFile
		return f, json.NewDecoder(resp.Body).Decode(&f)
	}
}
