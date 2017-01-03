package log

import (
	"github.com/sirupsen/logrus"
)

type Logger struct {
	log *logrus.Entry
}

func InitLogger(debug bool) *Logger {

	l := new(Logger)

	lg := logrus.New()

	if debug {
		lg.Level = logrus.DebugLevel
	}

	l.log = lg.WithField("source", "dispatcher")

	return l

}

func (l *Logger) Info(args ...interface{}) {
	l.log.Info(args)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.log.Infof(format, args)
}

func (l *Logger) Debug(args ...interface{}) {
	l.log.Debug(args)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log.Debugf(format, args)
}

func (l *Logger) Error(args ...interface{}) {
	l.log.Error(args)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log.Errorf(format, args)
}
