package logger

import (
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func SetLevel(level logrus.Level) {
	log.SetLevel(level)
}

func SetFormatter(formatter logrus.Formatter) {
	log.SetFormatter(formatter)
}

func Debug(msg string, keysAndValues ...any) {
	if len(keysAndValues) > 0 {
		log.WithFields(toFields(keysAndValues)).Debug(msg)
	} else {
		log.Debug(msg)
	}
}

func Info(msg string, keysAndValues ...any) {
	if len(keysAndValues) > 0 {
		log.WithFields(toFields(keysAndValues)).Info(msg)
	} else {
		log.Info(msg)
	}
}

func Warn(msg string, keysAndValues ...any) {
	if len(keysAndValues) > 0 {
		log.WithFields(toFields(keysAndValues)).Warn(msg)
	} else {
		log.Warn(msg)
	}
}

func Error(msg string, keysAndValues ...any) {
	if len(keysAndValues) > 0 {
		log.WithFields(toFields(keysAndValues)).Error(msg)
	} else {
		log.Error(msg)
	}
}

func toFields(keysAndValues []any) logrus.Fields {
	fields := make(logrus.Fields)
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key, ok := keysAndValues[i].(string)
			if ok {
				fields[key] = keysAndValues[i+1]
			}
		}
	}
	return fields
}
