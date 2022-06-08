package main

import (
    _ "strings"
    _ "strconv"
    "path/filepath"
    
    "github.com/tkanos/gonfig"
)

type Receiver struct {
    QUEUESIZE       int
    PORT            int
    TYPE            string
    BINDADDRESS     string
    SYSLOGFORMAT    string
}
    
type Emitter struct {
    QUEUESIZE       int
    PORT            int
    TYPE            string
    BINDADDRESS     string
    SYSLOGFORMAT    string
}
    
type RemoteLogging struct {
    QUEUESIZE       int
    PORT            int
    TYPE            string
    BINDADDRESS     string
    SYSLOGFORMAT    string
}

type DbWorker struct {
    FILENAME        string
    GARBAGETIMEOUTMIN  int
    REGULARDUMPINGSEC  int
}

type MsgMetadata struct {
    Hostname        string
}

type MsgType struct {
    START           string
    STOP            string
    TRIGGER         string
}

type MsgData        map[string]string

type Mapping struct {
    MsgMetadata     MsgMetadata
    MsgType         MsgType
    MsgData         MsgData
    MsgBlacklist    []string
    MSGMERGE        string
    MSGKEY          string
    MSGDATAENRICH   string
    MSGRAW          string
}

type Parser struct {
    QUEUESIZE       int
    Mapping         Mapping
}

type AppLogger struct {
    LEVEL           int
    FILENAME        string
    APPNAME         string
    RemoteLogging   RemoteLogging
}

type Configuration struct {
    Receiver        Receiver
    Emitter         Emitter
    DbWorker        DbWorker
    Parser          Parser
    AppLogger       AppLogger
}

type Orchestrator struct {  
    CONFIGFILE      string
    Config          Configuration
}

func (o *Orchestrator) New(configFile string) {
	o.CONFIGFILE = filepath.Join("storage", configFile)
    o.LoadConfigurationFromFile()
}

func (o *Orchestrator) LoadConfigurationFromFile() {
    err := gonfig.GetConf(o.CONFIGFILE, &o.Config)
    if err != nil {
        panic(err)
    }
}