package main

import (
    "fmt"
    "strings"
    "strconv"

	"gopkg.in/mcuadros/go-syslog.v2"
    "gopkg.in/mcuadros/go-syslog.v2/format"
)

type SyslogListener struct {  
    SyslogFmt        format.Format
    SERVERTYPE       string
    SERVERADDRESS    string
    SERVERPORT       int
}

func (sl *SyslogListener) New( srvAddr string, srvPrt int, srvType string, SyslogFmt string) {
	sl.SERVERADDRESS = srvAddr
	sl.SERVERPORT = srvPrt
	sl.SERVERTYPE = srvType
    sl.SyslogFmt = sl.StringToSyslogFormat(SyslogFmt)
}

func (sl *SyslogListener) Run(listenerQueue chan format.LogParts) {
    var err error
	channel := make(syslog.LogPartsChannel)
	handler := syslog.NewChannelHandler(channel)
	
	server := syslog.NewServer()
	server.SetFormat(sl.SyslogFmt)
	server.SetHandler(handler)

    var tmpPort = strconv.Itoa( sl.SERVERPORT )

	var tmpString strings.Builder
	tmpString.WriteString(sl.SERVERADDRESS)
	tmpString.WriteString(":")
	tmpString.WriteString( tmpPort )

    var tmpSrvType = strings.ToLower(sl.SERVERTYPE)

	if strings.Contains(tmpSrvType, "tcp") {
		err = server.ListenTCP(tmpString.String())
	} else {
		err = server.ListenUDP(tmpString.String())
	}
    
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
    
	tmpString.WriteString("; Format: ")
	tmpString.WriteString( sl.SyslogFormatToString() )
    var output strings.Builder
	output.WriteString("Syslog Server on: ")
	output.WriteString(tmpString.String())
    ComLogger.Write("I", output.String())

	err = server.Boot()
    
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}

	go func(channel syslog.LogPartsChannel) {
        for {
            select {
            case logParts := <-channel:
                listenerQueue <- logParts
            }
        }
	}(channel)

	server.Wait()
    
    defer server.Kill()
}

func (sl *SyslogListener) SyslogFormatToString() string {
    var ret string
    if ret = "RFC3164"; sl.SyslogFmt != syslog.RFC3164 {
        ret = "RFC5424"
    }
    return ret
}

func (sl *SyslogListener) StringToSyslogFormat(SyslogFmt string) format.Format {
    var ret format.Format
    if ret = syslog.RFC3164; SyslogFmt != "RFC3164" {
        ret = syslog.RFC5424
    }
    return ret
}