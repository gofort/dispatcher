package log

import (
	"github.com/sirupsen/logrus"
	"runtime"
	"path"
	"fmt"
)

type Logger struct {
	log *logrus.Entry
}

type CallerInfoHook struct {}

func (hook CallerInfoHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook CallerInfoHook) Fire(entry *logrus.Entry) error {

	if pc, file, line, ok := runtime.Caller(6); ok {

		funcName := runtime.FuncForPC(pc).Name()

		entry.Data["file"] = fmt.Sprintf("%s:%d", path.Base(file), line)
		entry.Data["func"] = path.Base(funcName)

	}

	return nil
}

func InitLogger(debug bool) *Logger {

	l := new(Logger)

	lg := logrus.New()
	lg.Hooks.Add(&CallerInfoHook{})

	l.log = lg.WithField("source", "dispatcher")

	if debug {
		l.log.Level = logrus.DebugLevel
	}

	return l

}

func (l *Logger) Info(args ...interface{}) {
	l.log.Info(args)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.log.Infof(format, args)
}

func (l *Logger) Infoln(args ...interface{}) {
	l.log.Infoln(args)
}

func (l *Logger) Debug(args ...interface{}) {
	l.log.Debug(args)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log.Debugf(format, args)
}

func (l *Logger) Debugln(args ...interface{}) {
	l.log.Debugln(args)
}

func (l *Logger) Error(args ...interface{}) {
	l.log.Error(args)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log.Errorf(format, args)
}

func (l *Logger) Errorln(args ...interface{}) {
	l.log.Errorln(args)
}
