package main

import (
    "fmt"
    "strings"
    "strconv"
    
    syslog "github.com/RackSec/srslog"
)

type SyslogSender struct {
    SERVERTYPE       string
    SERVERADDRESS    string
    SERVERPORT       int
    FORMAT           string
    APPNAME          string
    SysOut           *syslog.Writer
}

func (ss *SyslogSender) New(appName string, srvAddr string, srvPrt int, srvType string, syslogFormat string) {
    ss.SERVERADDRESS = srvAddr
    ss.SERVERPORT = srvPrt
    ss.SERVERTYPE = srvType
    ss.FORMAT = syslogFormat
    ss.APPNAME = appName
    
    ss.Create()
}

func (ss *SyslogSender) Run(outEngineQueue chan string) {
    for {
        select {
        case line := <-outEngineQueue:
            ComLogger.Write("I", fmt.Sprintf("Sending enriched syslog: %s", line))
            ss.SysOut.Info(line)
        }
    }
}

func (ss *SyslogSender) GetSyslog() *syslog.Writer {
    return ss.SysOut
}

func (ss *SyslogSender) Quit() {
    ss.SysOut.Close()
    fmt.Println("Sender quit...")
}

func (ss *SyslogSender) Create() {
    var tmpPort = strconv.Itoa( ss.SERVERPORT )
    var err error

	var tmpString strings.Builder
	tmpString.WriteString(ss.SERVERADDRESS)
	tmpString.WriteString(":")
	tmpString.WriteString( tmpPort )
    
    ss.SysOut, err = syslog.Dial(strings.ToLower(ss.SERVERTYPE), tmpString.String(), syslog.LOG_ERR, ss.APPNAME)
    if err != nil {
        ComLogger.Write("E", fmt.Sprintf("Failed to connect to syslog: %s", err.Error()))
    }
    
	tmpString.WriteString("; Format: ")
	tmpString.WriteString( ss.FORMAT )
    var output strings.Builder
	output.WriteString("Syslog Client [")
	output.WriteString(ss.APPNAME)
	output.WriteString("] on: ")
	output.WriteString(tmpString.String())
    
    if ss.FORMAT == "RFC3164" {
        //ss.SysOut.SetFormatter(syslog.RFC5424Formatter)
    } else if ss.FORMAT == "RFC5424" {
        ss.SysOut.SetFormatter(syslog.RFC5424Formatter)
    }
    
    ComLogger.Write("I", output.String())
}