package main

import (
	"log"
    "strings"
    "io"
    "os"

	"gopkg.in/natefinch/lumberjack.v2"
)

type RotatingWriter struct {
    AppLogger         AppLogger
    remoteLogger      SyslogSender
}

func (rw *RotatingWriter) New(config AppLogger) {
	rw.AppLogger = config
    
    lumberjack := &lumberjack.Logger{
                            Filename:   rw.AppLogger.FILENAME,
                            MaxSize:    5, // megabytes after which new file is created
                            MaxBackups: 5, // number of backups
                            MaxAge:     60, //days
                        }
    
    log.SetOutput(lumberjack)
    var out io.Writer
    
    if len(rw.AppLogger.RemoteLogging.BINDADDRESS) > 0 {
        var tmpString strings.Builder
        tmpString.WriteString("internal")
        tmpString.WriteString(rw.AppLogger.APPNAME)
        
        rw.remoteLogger = SyslogSender{}
        rw.remoteLogger.New( tmpString.String(), rw.AppLogger.RemoteLogging.BINDADDRESS, rw.AppLogger.RemoteLogging.PORT, rw.AppLogger.RemoteLogging.TYPE, rw.AppLogger.RemoteLogging.SYSLOGFORMAT )
    
        if rw.AppLogger.LEVEL >= 5 {
            out = io.MultiWriter(lumberjack, rw.remoteLogger.GetSyslog(), os.Stdout)
        } else {
            out = io.MultiWriter(lumberjack, rw.remoteLogger.GetSyslog())
        }
    } else if rw.AppLogger.LEVEL >= 5 {
        out = io.MultiWriter(lumberjack, os.Stdout)
    } else {
        return
    }
    
    log.SetOutput(out)
}

func (rw *RotatingWriter) Write(lvl string, content string) {
	var tmpString strings.Builder
    var toSend bool = false
    
    if lvl == "E" || lvl == "ERR" || lvl == "ERROR" {
        toSend = true
        tmpString.WriteString("ERROR: ")
    } else if (lvl == "W" || lvl == "WARN") && rw.AppLogger.LEVEL >= 1 {
        toSend = true
        tmpString.WriteString("WARN: ")
    } else if (lvl == "I" || lvl == "INFO") && rw.AppLogger.LEVEL >= 2 {
        toSend = true
        tmpString.WriteString("INFO: ")
    } else if (lvl == "D" || lvl == "DEBUG") && rw.AppLogger.LEVEL >= 3 {
        toSend = true
        tmpString.WriteString("DEBUG: ")
    } else if (lvl == "V" || lvl == "VERBOSE") && rw.AppLogger.LEVEL >= 4 {
        toSend = true
        tmpString.WriteString("VERBOSE: ")
    }
    
    if toSend == true {
        tmpString.WriteString(content)
        log.Println(tmpString.String())
    }
}