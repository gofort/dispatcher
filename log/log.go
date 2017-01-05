package log

import (
	"github.com/sirupsen/logrus"
)

// Default dispatcher logger which is a wrapper around logrus.
type Logger struct {
	log *logrus.Entry
}

// Creates new logger instance.
// Adds logrus field: source=dispatcher.
func InitLogger(debug bool) *Logger {

	l := new(Logger)

	lg := logrus.New()

	if debug {
		lg.Level = logrus.DebugLevel
	}

	l.log = lg.WithField("source", "dispatcher")

	return l

}

// Interesting runtime events (startup/shutdown). Expect these to be immediately visible on a console, so be conservative and keep to a minimum.
func (l *Logger) Info(args ...interface{}) {
	l.log.Info(args)
}

// Interesting runtime events (startup/shutdown). Expect these to be immediately visible on a console, so be conservative and keep to a minimum.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.log.Infof(format, args)
}

// Detailed information on flow of through the system. Expect these to be written to logs only.
func (l *Logger) Debug(args ...interface{}) {
	l.log.Debug(args)
}

// Detailed information on flow of through the system. Expect these to be written to logs only.
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log.Debugf(format, args)
}

// Other runtime errors or unexpected conditions. Expect these to be immediately visible on a status console.
func (l *Logger) Error(args ...interface{}) {
	l.log.Error(args)
}

// Other runtime errors or unexpected conditions. Expect these to be immediately visible on a status console.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log.Errorf(format, args)
}
