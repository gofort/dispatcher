package dispatcher

// Log is an interface of logger which is used in dispatcher.
// By default dispatcher uses logrus.
// You can pass your own logger which fits this interface to dispatcher in server config.
type Log interface {
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}
