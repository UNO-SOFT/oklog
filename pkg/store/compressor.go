package store

import (
	"compress/gzip"
	"io"
	"strings"

	"github.com/oklog/oklog/pkg/fs"
)

type compressor string

const (
	noCompressor   = compressor("")
	gzipCompressor = compressor(".gzip")
)

func (c compressor) Ext() string {
	return string(c)
}

func (c compressor) Bare(path string) string {
	return strings.TrimSuffix(path, string(c))
}
func (c compressor) With(path string) string {
	if c == "" {
		return path
	}
	ext := string(c)
	if strings.HasSuffix(path, ext) {
		return path
	}
	return path + ext
}
func (c compressor) From(path string) compressor {
	if strings.HasSuffix(path, string(gzipCompressor)) {
		return gzipCompressor
	}
	return noCompressor
}

func (c compressor) NewReader(f fs.File) fs.File {
	if c == noCompressor {
		return f
	}
	return &compressedFile{File: f, compressor: c}
}
func (c compressor) NewWriter(f fs.File) fs.File {
	if c == noCompressor {
		return f
	}
	return &compressedFile{File: f, compressor: c}
}

type compressedFile struct {
	fs.File
	compressor
	r io.ReadCloser
	w io.WriteCloser
}

func (c *compressedFile) Name() string {
	return c.compressor.With(c.File.Name())
}

func (c *compressedFile) Read(p []byte) (int, error) {
	if c.r == nil {
		switch c.compressor {
		case gzipCompressor:
			var err error
			if c.r, err = gzip.NewReader(c.File); err != nil {
				return 0, err
			}
		default:
			c.r = c.File
		}
	}
	return c.r.Read(p)
}

func (c *compressedFile) Write(p []byte) (int, error) {
	if c.w == nil {
		switch c.compressor {
		case gzipCompressor:
			var err error
			if c.w, err = gzip.NewWriterLevel(c.File, gzip.BestSpeed); err != nil {
				return 0, err
			}
		default:
			c.w = c.File
		}
	}
	return c.w.Write(p)
}

func (c *compressedFile) Close() error {
	r, w := c.r, c.w
	c.r, c.w = nil, nil
	var err error
	if r != nil {
		if e := r.Close(); e != nil && err == nil {
			err = e
		}
	}
	if w != nil {
		if e := w.Close(); e != nil && err == nil {
			err = e
		}
	}
	if e := c.File.Close(); e != nil && err == nil {
		err = e
	}
	return err
}
