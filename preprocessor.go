package main

import (
	"fmt"
	_ "log"
	_ "os"
    "regexp"

    "gopkg.in/mcuadros/go-syslog.v2/format"
)

type Preprocessor struct {
    Orchestra       Orchestrator
    DbWorker        SqliteWorker
    Receiver        SyslogListener
    Sender          SyslogSender
    listenerQueue   chan format.LogParts
    inEngineQueue   chan Mapping
    outEngineQueue  chan string
    waitForTrigger  bool
    sendByInterval  bool
}

var ComLogger *RotatingWriter

func (pp *Preprocessor) New(configFile string) {
    // Initiates Orchestrator
    pp.Orchestra = Orchestrator{}
    pp.Orchestra.New(configFile)
    
    // Initiates Logger
    ComLogger = &RotatingWriter{}
    ComLogger.New(pp.Orchestra.Config.AppLogger)
    
    ComLogger.Write("D", fmt.Sprintf("Current Config: %#v", pp.Orchestra.Config) )
    
    // Setting up channels & queues
    pp.listenerQueue = make(chan format.LogParts, pp.Orchestra.Config.Receiver.QUEUESIZE)
    pp.inEngineQueue = make(chan Mapping, pp.Orchestra.Config.Parser.QUEUESIZE * 2)
    pp.outEngineQueue = make(chan string, pp.Orchestra.Config.Emitter.QUEUESIZE * 2)
    
    pp.waitForTrigger = false
    pp.sendByInterval = false
    
    var dmpIval int = 0
    
    if pp.Orchestra.Config.DbWorker.REGULARDUMPINGSEC > 0 {
        pp.sendByInterval = true
        dmpIval = pp.Orchestra.Config.DbWorker.REGULARDUMPINGSEC
    } else if len(pp.Orchestra.Config.Parser.Mapping.MsgType.TRIGGER) > 0 {
        pp.waitForTrigger = true
    }
    
    // SQLite Worker to unload intermediary queue (parsed items)
    pp.DbWorker = SqliteWorker{}
    pp.DbWorker.New(pp.Orchestra.Config.DbWorker.FILENAME, pp.Orchestra.Config.Parser.Mapping.MsgData, pp.Orchestra.Config.Parser.Mapping.MSGKEY, pp.Orchestra.Config.Parser.Mapping.MSGDATAENRICH, pp.sendByInterval, pp.waitForTrigger, pp.Orchestra.Config.DbWorker.GARBAGETIMEOUTMIN, dmpIval)
    
    // Define listening thread for outgoing messages
    pp.Sender = SyslogSender{}
    pp.Sender.New( pp.Orchestra.Config.AppLogger.APPNAME, pp.Orchestra.Config.Emitter.BINDADDRESS, pp.Orchestra.Config.Emitter.PORT, pp.Orchestra.Config.Emitter.TYPE, pp.Orchestra.Config.Emitter.SYSLOGFORMAT )
    
    // Define blocking thread for incoming messages
    pp.Receiver = SyslogListener{}
    pp.Receiver.New( pp.Orchestra.Config.Receiver.BINDADDRESS, pp.Orchestra.Config.Receiver.PORT, pp.Orchestra.Config.Receiver.TYPE, pp.Orchestra.Config.Receiver.SYSLOGFORMAT)
    
    // Cleaning and monitor memory footprint, logging, etc
}

func (pp *Preprocessor) Run() {
    // Deal with the queue now that it's filling up (parsing and extraction of valuable data)
    go pp.Parser()
    // Deal with the queue now that it's filling up (logs to emit)
    go pp.Sender.Run(pp.outEngineQueue)
    
    // SQLite Worker to unload intermediary queue (parsed items)
    go pp.DbWorker.Run(pp.inEngineQueue, pp.outEngineQueue)
    
    // Timeout or log completed: loads on output Queue for emission
    
    // Testing
    //testUdpClient := Udpclienttest{}
    //testUdpClient.New( pp.Orchestra.Config.Receiver.BINDADDRESS, pp.Orchestra.Config.Receiver.PORT, pp.Orchestra.Config.Receiver.TYPE, pp.Orchestra.Config.Receiver.SYSLOGFORMAT)
    //go testUdpClient.Run()
    
    // Start blocking thread for incoming messages
    pp.Receiver.Run( pp.listenerQueue )
    
    // Cleaning and monitor memory footprint, logging, etc
    pp.DbWorker.Quit()
}

func (pp *Preprocessor) Parser() {
    for {
        select {
        case line := <-pp.listenerQueue:
            msgMapped := Mapping{}
            message := ""
            
            if pp.Orchestra.Config.Receiver.SYSLOGFORMAT == "RFC3164" {
                message = line["content"].(string)
            } else if pp.Orchestra.Config.Receiver.SYSLOGFORMAT == "RFC5424" {
                message = line["message"].(string)
            } else {
                break
            }
            
            if pp.CheckBlacklistForMsg( message ) == false {
            
                msgType := pp.GetTypeFromMsg( message )
                msgMapped.MSGMERGE = pp.GetIdFromMsg( message )
                msgMapped.MsgMetadata.Hostname = pp.GetHostnameFromMsg(line, message)
                msgMapped.MsgData = pp.GetDataFromMsg( message )
                msgMapped.MSGRAW = message
                
                if len(msgType) > 0 {
                    // Trigger add, push, pull or purge DB entry for MSG IDmsgMapped.MsgType
                    if msgType == "START" {
                        msgMapped.MsgType.START = msgType
                    } else if msgType == "TRIGGER" {
                        msgMapped.MsgType.TRIGGER = msgType
                    } else if msgType == "STOP" {
                        msgMapped.MsgType.STOP = msgType
                    }
                }
                pp.inEngineQueue <- msgMapped
                
            } else {
                continue
            }
        }
    }
    fmt.Println("Parser quit...")
}

func (pp *Preprocessor) GetTypeFromMsg(msg string) string {
    retType := ""
    re := regexp.MustCompile(pp.Orchestra.Config.Parser.Mapping.MsgType.START)
    
    if len(re.Find([]byte(msg))) > 0 {
        retType = "START"
    }
    
    if len(pp.Orchestra.Config.Parser.Mapping.MsgType.STOP) > 0 {
        re = regexp.MustCompile(pp.Orchestra.Config.Parser.Mapping.MsgType.STOP)
        
        if len(re.Find([]byte(msg))) > 0 {
            retType = "STOP"
        }
    }
    
    if len(pp.Orchestra.Config.Parser.Mapping.MsgType.TRIGGER) > 0 {
        re = regexp.MustCompile(pp.Orchestra.Config.Parser.Mapping.MsgType.TRIGGER)
        
        if len(re.Find([]byte(msg))) > 0 {
            retType = "TRIGGER"
        }
    }
    
    return retType
}

func (pp *Preprocessor) GetIdFromMsg(msg string) string {
    retId := ""
    re := regexp.MustCompile(pp.Orchestra.Config.Parser.Mapping.MSGMERGE)
    
    if len(re.Find([]byte(msg))) > 0 {
        find := re.FindAllStringSubmatch(msg, -1)
        
        if len(find[0]) > 1 {
            retId = find[0][1]
        }
    }
    
    return retId
}

func (pp *Preprocessor) GetDataFromMsg(msg string) map[string]string {
    refData := pp.Orchestra.Config.Parser.Mapping.MsgData
    retData := MsgData{}
    
    for key, value := range refData {
        re := regexp.MustCompile(value)
        
        if len(re.Find([]byte(msg))) > 0 {
            find := re.FindAllStringSubmatch(msg, -1)
            
            if len(find[0]) > 1 {
                retData[key] = find[0][1]
            } else {
                retData[key] = ""
            }
        }
    }
    
    return retData
}

func (pp *Preprocessor) GetHostnameFromMsg(line format.LogParts, msg string) string {
    retVal := ""
    
    if len(pp.Orchestra.Config.Parser.Mapping.MsgMetadata.Hostname) > 0 {
        re := regexp.MustCompile(pp.Orchestra.Config.Parser.Mapping.MsgMetadata.Hostname)

        if len(re.Find([]byte(msg))) > 0 {
            find := re.FindAllStringSubmatch(msg, -1)
            
            if len(find[0]) > 1 {
                retVal = find[0][1]
            } else {
                retVal = line["hostname"].(string)
            }
        } else {
            retVal = line["hostname"].(string)
        }
    } else {
        retVal = line["hostname"].(string)
    }
    
    return retVal
}

func (pp *Preprocessor) CheckBlacklistForMsg(msg string) bool {
    refData := pp.Orchestra.Config.Parser.Mapping.MsgBlacklist
    retData := false
    
    for _, value := range refData {
        re := regexp.MustCompile(value)
        
        if len(re.Find([]byte(msg))) > 0 {
            retData = true
            break
        }
    }
    
    return retData
}

