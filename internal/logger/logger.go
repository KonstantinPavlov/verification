package logger

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"runtime"
	"strings"

	"github.com/KonstantinPavlov/verification/internal/logger/utils"
	slogenv "github.com/cbrewster/slog-env"
)

var cleanPattern = regexp.MustCompile(`(\r)|(\n)|(\t)`)

type HandlerBuilder interface {
	Build(w io.Writer) slog.Handler
}

type Logger struct {
	*slog.Logger
}

func New(hs ...slog.Handler) (l *Logger) {

	var h slog.Handler
	switch {
	case len(hs) == 1 && hs[0] != nil:
		h = hs[0]
	case len(hs) > 1:
		h = &teeHandler{hs}
	default:
		h = NewDefaultBuilder().LevelStr("DEBUG").Build(os.Stdout)
	}
	return &Logger{Logger: slog.New(slogenv.NewHandler(h))}
}

func (l *Logger) WithAttrs(attrs []slog.Attr) *Logger {
	args := make([]any, 0, len(attrs)*2)
	for _, a := range attrs {
		args = append(args, a.Key, a.Value)
	}
	return &Logger{Logger: l.Logger.With(args...)}
}

func (l *Logger) WithGroup(name string) *Logger {
	return &Logger{Logger: l.Logger.WithGroup(name)}
}

type groupedAttrs struct {
	group string
	attrs []slog.Attr
}

type commonHandler struct {
	grAs []groupedAttrs
	opts *slog.HandlerOptions
	out  io.Writer
}

func newCommonHandler(w io.Writer, opts *slog.HandlerOptions) (ch *commonHandler) {
	return &commonHandler{
		opts: opts,
		out:  utils.NewSafeWriter(w),
	}
}

func (ch *commonHandler) copy() (newCh *commonHandler) {
	return &commonHandler{
		opts: ch.opts,
		out:  ch.out,
	}
}

func (ch *commonHandler) withGroupedAttrs(grAs groupedAttrs) (newCh *commonHandler) {
	newCh = ch.copy()
	newCh.grAs = make([]groupedAttrs, len(ch.grAs)+1)
	copy(newCh.grAs, ch.grAs)
	newCh.grAs[len(newCh.grAs)-1] = grAs
	return newCh
}

func (ch *commonHandler) writeLine(p []byte) (err error) {
	_, err = ch.out.Write(append(cleanPattern.ReplaceAll(p, []byte{' '}), '\n'))
	return err
}

func (ch *commonHandler) forEachGropedAttrs(fn func(attr slog.Attr)) (majorGroup string) {
	var majorSb strings.Builder
	var keySb strings.Builder

	for _, ga := range ch.grAs {
		if ga.group != "" {
			if majorSb.Len() > 0 {
				majorSb.WriteByte('.')
			}
			majorSb.WriteString(ga.group)
		}

		majorGroup = majorSb.String()

		for _, a := range ga.attrs {
			if majorGroup != "" {
				keySb.WriteString(majorGroup)
				keySb.WriteByte('.')
				keySb.WriteString(a.Key)
			} else {
				keySb.WriteString(a.Key)
			}
			fn(slog.Attr{Key: keySb.String(), Value: a.Value})
			keySb.Reset()
		}
	}
	return majorGroup
}

func (ch *commonHandler) forEachRecordAttrs(majorGroup string, fn func(attr slog.Attr)) func(slog.Attr) bool {
	return func(a slog.Attr) bool {
		if majorGroup != "" {
			a.Key = majorGroup + "." + a.Key
		}
		fn(a)
		return true
	}
}

func StrLevel(s string) (level slog.Leveler) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "error":
		return slog.LevelError
	case "warn":
		return slog.LevelWarn
	default:
		return slog.LevelInfo
	}
}

func attrValueToString(v slog.Value) (s string) {
	switch v.Kind() {
	case slog.KindBool, slog.KindFloat64, slog.KindInt64, slog.KindUint64:
		return fmt.Sprintf(`%v`, v.Any())
	default:
		return fmt.Sprintf(`"%v"`, v.Any())
	}
}

type logLineChain func(buff *bytes.Buffer, r slog.Record) (delimeterAfter bool)

func getGoroutineId() []byte {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	if idx := bytes.IndexByte(buf, '['); idx != -1 {
		buf = buf[:idx]
		return bytes.ReplaceAll(bytes.TrimSpace(buf), []byte{' '}, []byte{'-'})
	}
	return []byte{}
}
