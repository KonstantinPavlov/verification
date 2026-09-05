package logger

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
)

type DefaultHandler struct {
	*commonHandler
}

func (dh *DefaultHandler) Enabled(_ context.Context, level slog.Level) (ok bool) {
	return level >= dh.commonHandler.opts.Level.Level()
}

func (dh *DefaultHandler) Handle(ctx context.Context, r slog.Record) (err error) {
	outBuff := bytes.NewBuffer([]byte{})
	for _, fn := range []logLineChain{
		dh.addTime(),
		dh.addLevel(),
		dh.addGorutine(),
		dh.addSource(),
		dh.addMDC(),
		dh.addMessage(),
	} {
		if fn(outBuff, r) {
			outBuff.WriteByte(' ')
		}
	}
	return dh.commonHandler.writeLine(outBuff.Bytes())
}

func (dh *DefaultHandler) WithGroup(name string) (h slog.Handler) {
	if name == "" {
		return dh
	}
	return &DefaultHandler{
		commonHandler: dh.commonHandler.withGroupedAttrs(groupedAttrs{group: name}),
	}
}

func (dh *DefaultHandler) WithAttrs(attrs []slog.Attr) (h slog.Handler) {
	if len(attrs) == 0 {
		return dh
	}
	return &DefaultHandler{
		commonHandler: dh.commonHandler.withGroupedAttrs(groupedAttrs{attrs: attrs}),
	}
}

func (dh *DefaultHandler) addTime() logLineChain {
	return func(buff *bytes.Buffer, r slog.Record) (ok bool) {
		if !r.Time.IsZero() {
			buff.WriteString(r.Time.Format("2006-01-02 15:04:05,000"))
		}
		return true
	}
}

func (dh *DefaultHandler) addGorutine() logLineChain {
	return func(buff *bytes.Buffer, r slog.Record) (ok bool) {
		buff.WriteByte('[')
		buff.Write(getGoroutineId())
		buff.WriteByte(']')
		return true
	}
}

func (dh *DefaultHandler) addLevel() logLineChain {
	return func(buff *bytes.Buffer, r slog.Record) (ok bool) {
		buff.WriteByte('[')
		buff.WriteString(r.Level.Level().String())
		buff.WriteByte(']')
		return true
	}
}

func (dh *DefaultHandler) addSource() logLineChain {
	return func(buff *bytes.Buffer, r slog.Record) (ok bool) {
		if dh.opts.AddSource {
			fs := runtime.CallersFrames([]uintptr{r.PC})
			f, _ := fs.Next()
			file := filepath.Base(f.File)
			buff.WriteByte('[')
			buff.WriteString(file)
			buff.Write([]byte{':', ':'})
			buff.WriteString(f.Function)
			buff.WriteByte(':')
			buff.WriteString(fmt.Sprint(f.Line))
			buff.WriteByte(']')
		}
		return true
	}
}

func (dh *DefaultHandler) addMDC() logLineChain {
	return func(buff *bytes.Buffer, r slog.Record) (ok bool) {
		buff.WriteString("(")
		strtMDC := buff.Len()
		majorGroup := dh.commonHandler.forEachGropedAttrs(func(attr slog.Attr) {
			buff.WriteString(dh.attrToString(attr))
			buff.Write([]byte{',', ' '})
		})
		r.Attrs((dh.commonHandler.forEachRecordAttrs(
			majorGroup,
			func(attr slog.Attr) {
				buff.WriteString(dh.attrToString(attr))
				buff.Write([]byte{',', ' '})
			},
		)))
		if buff.Len()-strtMDC > 2 {
			buff.Truncate(buff.Len() - 2)
		}
		buff.Write([]byte{')', '|'})
		return true
	}
}

func (dh *DefaultHandler) addMessage() logLineChain {
	return func(buff *bytes.Buffer, r slog.Record) (ok bool) {
		buff.WriteString(r.Message)
		return true
	}
}

func (dh *DefaultHandler) attrToString(attr slog.Attr) (s string) {
	buff := new(bytes.Buffer)
	buff.WriteString(attr.Key)
	buff.WriteString("=")
	buff.WriteString(attrValueToString(attr.Value))
	return buff.String()
}
