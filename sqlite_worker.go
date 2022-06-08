package main

import (
	"fmt"
	"os"
    "strings"
    "crypto/sha1"
    "encoding/hex"
    "reflect"
    "time"
    
    "database/sql"
	_ "github.com/mattn/go-sqlite3"
)

type Key map[string]string

type FullMessage struct {  
    Id            string
    Hostname      string
    Mapped        MsgData
}

type SqliteWorker struct {  
    DBFILENAME     string
    TABLENAME      string
    GBTG           int
    DMPIVAL        int
    DbObject       *sql.DB
    waitForTrigger bool
    sendByInterval bool
    keyStruct      []string
    outStruct      []string
    dataStruct     []string
    vac            bool
    dmp            bool
    gb             bool
    outEngineQueue chan string
}

func (sw *SqliteWorker) New(dbFile string, dataStruct MsgData, keyStruct string, outStruct string, sendByInterval bool, waitForTrigger bool, gbTrigger int, dumpIval int) {
    ComLogger.Write( "I", fmt.Sprintf("Sqlite Engine initialized, waiting for trigger '%t', send by interval '%t'", waitForTrigger, sendByInterval) )
    
    var err error
	sw.DBFILENAME = dbFile
    
    var fullDbFilename strings.Builder
    fullDbFilename.WriteString(dbFile)
    fullDbFilename.WriteString("?cache=shared&mode=rwc&_busy_timeout=1000")
    
	sw.waitForTrigger = waitForTrigger
    sw.sendByInterval = sendByInterval
    sw.vac = true
    sw.dmp = true
    sw.gb = true
    sw.GBTG = gbTrigger * 60
    sw.DMPIVAL = dumpIval
    
	sw.DbObject, err = sql.Open("sqlite3", fullDbFilename.String())
    sw.DbObject.SetMaxOpenConns(1)
    
	if err != nil {
        ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
    
    // Create table from struct Mapping
    var structString strings.Builder
    structString.WriteString("`id` TEXT NOT NULL")
    structString.WriteString(", `hostname` TEXT DEFAULT ''")
    structString.WriteString(", `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    structString.WriteString(", `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    structString.WriteString(", `emitted` INTEGER DEFAULT 0")
    
    for key, _ := range dataStruct {
        structString.WriteString(", `")
        structString.WriteString(key)
        structString.WriteString("` TEXT DEFAULT ''")
        sw.dataStruct = append(sw.dataStruct, key)
    }
    
    sw.keyStruct = strings.Split(keyStruct, ",")
    sw.outStruct = strings.Split(outStruct, ",")
    
    h := sha1.New()
    h.Write([]byte(structString.String()))
    
    sw.TABLENAME = hex.EncodeToString( h.Sum(nil) )
    
    if sw.CheckIfTableExists() == false {
        sw.NewTable(structString.String())
        sw.CreateIndexForKey()
    } else if sw.waitForTrigger == false && sw.sendByInterval == false {
        sw.PurgeTable()
    }
}

func (sw *SqliteWorker) CreateIndexForKey() {
    var err error
    var sqlStmt strings.Builder
    
    // Create index from Key struct
    sqlStmt.WriteString("CREATE INDEX idx_")
    sqlStmt.WriteString(sw.TABLENAME)
    sqlStmt.WriteString(" ON `")
    sqlStmt.WriteString(sw.TABLENAME)
    sqlStmt.WriteString("` (`id`")
    
    for _, v := range sw.keyStruct {
        if v != "id" {
            sqlStmt.WriteString(", `")
            sqlStmt.WriteString(v)
            sqlStmt.WriteString("`")
        }
    }
    
    sqlStmt.WriteString(");");
    
    ComLogger.Write( "I", fmt.Sprintf("New Index Created: %s", sqlStmt.String() ))
    
	_, err = sw.DbObject.Exec(sqlStmt.String())
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
}

func (sw *SqliteWorker) CloseDatabase() {
	sw.DbObject.Close()
}

func (sw *SqliteWorker) ResetDatabase() {
	os.Remove(sw.DBFILENAME)
}

func (sw *SqliteWorker) NewTable(structure string) {
    var err error
	var sqlStmt strings.Builder
    
    pragmaSetup := "PRAGMA page_size=4096; PRAGMA cache_size=-100000; PRAGMA auto_vacuum=INCREMENTAL; VACUUM;"
    
	sqlStmt.WriteString(pragmaSetup)
	sqlStmt.WriteString(" ")
	sqlStmt.WriteString("CREATE TABLE `")
	sqlStmt.WriteString(sw.TABLENAME)
	sqlStmt.WriteString("` (")
	sqlStmt.WriteString(structure)
	sqlStmt.WriteString(");")
    
    ComLogger.Write( "I", fmt.Sprintf("New DB Created: %s", sqlStmt.String() ))
    
	_, err = sw.DbObject.Exec(sqlStmt.String())
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
}

func (sw *SqliteWorker) Add(key Key) {
    var err error
	var sqlStmt strings.Builder
	var sqlStmt2 strings.Builder
    
	sqlStmt.WriteString("INSERT INTO `")
	sqlStmt.WriteString(sw.TABLENAME)
	sqlStmt.WriteString("` (`id`")
    
	sqlStmt2.WriteString(") VALUES ('")
	sqlStmt2.WriteString(key["id"])
	sqlStmt2.WriteString("'")
    
    for k, v := range key {
        if k != "id" {
            sqlStmt.WriteString(", `")
            sqlStmt.WriteString(k)
            sqlStmt.WriteString("`")
            
            sqlStmt2.WriteString(", '")
            sqlStmt2.WriteString(v)
            sqlStmt2.WriteString("'")
        }
    }
    
    sqlStmt2.WriteString(");")
	sqlStmt.WriteString(sqlStmt2.String())
    
    ComLogger.Write("D", sqlStmt.String())
    
	tx, err := sw.DbObject.Begin()
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
    
	stmt, err := tx.Prepare(sqlStmt.String())
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
	defer stmt.Close()
	
    _, err = stmt.Exec()
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
	defer stmt.Close()
    
	if err := tx.Commit(); err != nil {
        _ = tx.Rollback()
        ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
    }
}

func (sw *SqliteWorker) Push(key Key, col string, value string) int64 {
    var err error
	var sqlStmt strings.Builder
    
	sqlStmt.WriteString("UPDATE `")
	sqlStmt.WriteString(sw.TABLENAME)
	sqlStmt.WriteString("` SET `")
	sqlStmt.WriteString(col)
    sqlStmt.WriteString("` = ")
    
    if sw.waitForTrigger == false && sw.sendByInterval == false {
        sqlStmt.WriteString("?")
    } else {
        sqlStmt.WriteString("(`")
        sqlStmt.WriteString(col)
        sqlStmt.WriteString("` || ?)")
    }
    sqlStmt.WriteString(", `updated_at` = CURRENT_TIMESTAMP WHERE (`id` = ?")
    
    for k, v := range key {
        if k != "id" {
            sqlStmt.WriteString(" AND `")
            sqlStmt.WriteString(k)
            sqlStmt.WriteString("` = \"")
            sqlStmt.WriteString(v)
            sqlStmt.WriteString("\"")
        }
    }
    
    sqlStmt.WriteString(");")
    
    ComLogger.Write("D", sqlStmt.String())
    
	tx, err := sw.DbObject.Begin()
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
    
	stmt, err := tx.Prepare(sqlStmt.String())
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
	defer stmt.Close()
    
	res, err := stmt.Exec(value, key["id"])
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
	defer stmt.Close()
    
	affected, err := res.RowsAffected()
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
	defer stmt.Close()
    
	if err := tx.Commit(); err != nil {
        _ = tx.Rollback()
        ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
    }
    
    return affected
}

func (sw *SqliteWorker) SelectAll() {
    var err error
    var rows *sql.Rows

	rows, err = sw.DbObject.Query("select id, serverName from mappedData")
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
		}
		fmt.Println(id, name)
	}
    defer rows.Close()
    
	err = rows.Err()
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
}

func (sw *SqliteWorker) SelectKey(key Key) MsgData {
    var err error
    var stmt *sql.Stmt
    var first bool = true
    
	var sqlStmt strings.Builder
	sqlStmt.WriteString("SELECT ")
    
    for _, v := range sw.dataStruct {
        if first == false {
            sqlStmt.WriteString(", ")
        }
        sqlStmt.WriteString("`")
        sqlStmt.WriteString(v)
        sqlStmt.WriteString("`")
        first = false
    }
    
    sqlStmt.WriteString(" FROM `")
	sqlStmt.WriteString(sw.TABLENAME)
	sqlStmt.WriteString("` WHERE (`id` = ?")
    
    for k, v := range key {
        if k != "id" {
            sqlStmt.WriteString(" AND `")
            sqlStmt.WriteString(k)
            sqlStmt.WriteString("` = '")
            sqlStmt.WriteString(v)
            sqlStmt.WriteString("'")
        }
    }
    
    sqlStmt.WriteString(");")
    
    ComLogger.Write("D", sqlStmt.String())

	stmt, err = sw.DbObject.Prepare(sqlStmt.String())
    
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
	defer stmt.Close()
    
	rows, err := stmt.Query(key["id"])
    
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
	defer stmt.Close()
    
    msgData := MsgData{}
    cols, err := rows.Columns()

    for rows.Next(){
        row := make([]interface{}, 0)

        generic := reflect.TypeOf(row).Elem()

        for _ = range cols {
            row = append(row, reflect.New(generic).Interface())
        }
        _ = rows.Scan(row...)

        for i, col := range cols {
            msgData[col] = (*(row[i].(*interface{}))).(string)
        }
        
        break
    }
    defer rows.Close()
    
	return msgData
}

func (sw *SqliteWorker) SelectAllByDelay(delayMin int) []FullMessage {
    var err error
    var retList []FullMessage
    
	var sqlStmt strings.Builder
	sqlStmt.WriteString("SELECT `id`, `hostname`")
    
    for _, v := range sw.dataStruct {
        sqlStmt.WriteString(", ")
        sqlStmt.WriteString("`")
        sqlStmt.WriteString(v)
        sqlStmt.WriteString("`")
    }
    
    sqlStmt.WriteString(" FROM `")
    sqlStmt.WriteString(sw.TABLENAME)
    sqlStmt.WriteString("` WHERE ((strftime('%s', CURRENT_TIMESTAMP) - strftime('%s',`updated_at`)) >= ")
    sqlStmt.WriteString(fmt.Sprintf("%d", delayMin))
    sqlStmt.WriteString(") AND `emitted` = 0;")
    
    ComLogger.Write("D", sqlStmt.String())
    
	rows, err := sw.DbObject.Query(sqlStmt.String())
    
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
    
    cols, err := rows.Columns()
    
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}

    for rows.Next(){
        msg := FullMessage{}
        msg.Mapped = make(MsgData)
        
        row := make([]interface{}, 0)

        generic := reflect.TypeOf(row).Elem()

        for _ = range cols {
            row = append(row, reflect.New(generic).Interface())
        }
        _ = rows.Scan(row...)

        for i, col := range cols {
            if col == "id" {
                msg.Id = (*(row[i].(*interface{}))).(string)
            } else if col == "hostname" {
                msg.Hostname = (*(row[i].(*interface{}))).(string)
            } else {
                msg.Mapped[col] = (*(row[i].(*interface{}))).(string)
            }
        }
        retList = append(retList, msg)
    }
    defer rows.Close()
    
	return retList
}

func (sw *SqliteWorker) DeleteKey(key Key) {
    var err error
    var first bool
	var sqlStmt strings.Builder
    
	sqlStmt.WriteString("DELETE FROM `")
	sqlStmt.WriteString(sw.TABLENAME)
	sqlStmt.WriteString("` where (")
    
    first = true
    
    for k, v := range key {
        if first == false {
            sqlStmt.WriteString(" AND `")
        } else {
            first = false
            sqlStmt.WriteString(" `")
        }
        sqlStmt.WriteString(k)
        sqlStmt.WriteString("` = \"")
        sqlStmt.WriteString(v)
        sqlStmt.WriteString("\"")
    }
    
    sqlStmt.WriteString(");")
    
    ComLogger.Write("D", sqlStmt.String())
    
	_, err = sw.DbObject.Exec(sqlStmt.String())
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
}

func (sw *SqliteWorker) PurgeTable() {
    var err error
	var sqlStmt strings.Builder
    
	sqlStmt.WriteString("DELETE FROM `")
	sqlStmt.WriteString(sw.TABLENAME)
	sqlStmt.WriteString("`;")
    
    ComLogger.Write("D", sqlStmt.String())
    
	_, err = sw.DbObject.Exec(sqlStmt.String())
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
}

func (sw *SqliteWorker) TrimLeftChar(s string) string {
    for i := range s {
        if i > 0 {
            return s[i:]
        }
    }
    return s[:0]
}

func (sw *SqliteWorker) ForgeFromMsgData(m MsgData) string {
    // string factory + outEngineQueue
    var output strings.Builder
    var retVal string = ""
    
    for k, v := range m {
        if k != "payload" && len(v) > 0 {
            output.WriteString(" ")
            output.WriteString(k)
            output.WriteString("=")
            if strings.HasPrefix(v, " ") {
                v = sw.TrimLeftChar(v)
            }
            output.WriteString(v)
        }
    }
    
    if len(m["payload"]) > 0 {
        output.WriteString(" payload=")
        if strings.HasPrefix(m["payload"], " ") {
            m["payload"] = sw.TrimLeftChar(m["payload"])
        }
        output.WriteString(m["payload"])
    }
    
    if strings.HasPrefix(output.String(), " ") {
        retVal = sw.TrimLeftChar(output.String())
    } else {
        retVal = output.String()
    }
    
    return retVal
}

func (sw *SqliteWorker) ForgeFromFieldsList(k Key, f []string) string {
    // string factory + outEngineQueue
    
    storedData := sw.SelectKey(k)
    outputData := make(MsgData)
    
    if len(f) > 0 {
        for _, v := range f {
            if sw.StringInSlice(v, f) && len(storedData[v]) > 0 {
                outputData[v] = storedData[v]
            }
        }
    }
    
    return sw.ForgeFromMsgData(outputData)
}

func (sw *SqliteWorker) ForgeFromKey(k Key) string {
    // string factory + outEngineQueue
    var retVal string = ""
    
    storedData := sw.SelectKey(k)
    retVal = sw.ForgeFromMsgData(storedData)
    
    return retVal
}

func (sw *SqliteWorker) FillKeyMsgData(k Key, data MsgData) {
    // Enrich DB entry
    for j, v := range data {
        if len(v) > 0 && v != " " {
            if strings.HasPrefix(v, " ") == false {
                var tmp strings.Builder
                tmp.WriteString(" ")
                tmp.WriteString(v)
                v = tmp.String()
            }
            affected := sw.Push(k, j, v)
            if affected == 0 {
                ComLogger.Write("I", "Cannot update, create.")
                sw.Add(k)
                sw.Push(k, j, v)
            }
        }
    }
}

func (sw *SqliteWorker) StringInSlice(a string, list []string) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}

func (sw *SqliteWorker) AddFromMsgToForged( line Mapping, forged string, pushPayload bool ) string {
    var output strings.Builder
    
    output.WriteString("id=")
    output.WriteString(line.MSGMERGE)
    
    if sw.StringInSlice("hostname", sw.outStruct) {
        output.WriteString(" ")
        output.WriteString("hostname=")
        output.WriteString(line.MsgMetadata.Hostname)
    }
    
    if strings.HasPrefix(forged, " ") {
        output.WriteString(forged)
    } else {
        output.WriteString(" ")
        output.WriteString(forged)
    }
    
    if pushPayload {
        keys := make([]string, 0, len(line.MsgData))
        for k := range line.MsgData {
            keys = append(keys, k)
        }
        
        if sw.StringInSlice("payload", keys) {
            if strings.HasPrefix(line.MsgData["payload"], " ") || strings.HasSuffix(output.String(), " ") {
                output.WriteString("payload=")
                output.WriteString(line.MsgData["payload"])
            } else {
                output.WriteString(" ")
                output.WriteString("payload=")
                output.WriteString(line.MsgData["payload"])
            }
        }
    }
    
    return output.String()
}

func (sw *SqliteWorker) AddFromFullToForged( msg FullMessage, forged string ) string {
    var output strings.Builder
    
    output.WriteString("id=")
    output.WriteString(msg.Id)
    
    if sw.StringInSlice("hostname", sw.outStruct) {
        output.WriteString(" ")
        output.WriteString("hostname=")
        output.WriteString(msg.Hostname)
    }
    
    if strings.HasPrefix(forged, " ") {
        output.WriteString(forged)
    } else {
        output.WriteString(" ")
        output.WriteString(forged)
    }
    
    return output.String()
}

func (sw *SqliteWorker) Run(inEngineQueue chan Mapping, outEngineQueue chan string) {
    // treat queue inEngineQueue
    sw.outEngineQueue = outEngineQueue
    
    for {
        select {
        case line := <-inEngineQueue:
            ComLogger.Write("V", fmt.Sprintf("Received event: %#v", line))
            
            k := Key{}
            if len(sw.keyStruct) > 0 {
                for _, v := range sw.keyStruct {
                    if v == "id" && len(line.MSGMERGE) > 0 {
                        k[v] = line.MSGMERGE
                    } else if v == "hostname" {
                        k[v] = line.MsgMetadata.Hostname
                    } else {
                        k[v] = line.MsgData[v]
                    }
                }
            } else {
                k["id"] = line.MSGMERGE
            }
            
            // if needed populate outEngineQueue
            if len(line.MsgType.STOP) > 0 {
                // purge ID
                ComLogger.Write("V", "Stop: Purge enriched data")
                
                if sw.waitForTrigger == false && sw.sendByInterval == false {
                    // string factory + outEngineQueue
                    forged := sw.AddFromMsgToForged( line, sw.ForgeFromKey(k), true )
                    if len(forged) > 0 {
                        sw.outEngineQueue <- forged
                    }
                }
                
                sw.DeleteKey(k)
                
            } else if len(line.MsgType.START) > 0 {
                // store new ID
                ComLogger.Write("V", "Add event to Database")
                
                // store if anything else
                sw.FillKeyMsgData(k, line.MsgData)
                
                if sw.waitForTrigger == false && sw.sendByInterval == false {
                    // string factory + outEngineQueue
                    sw.outEngineQueue <- sw.AddFromMsgToForged( line, sw.ForgeFromMsgData(line.MsgData), false )
                }
                
            } else if len(line.MsgType.TRIGGER) > 0 && sw.waitForTrigger == true {
                // send enriched ID
                ComLogger.Write("V", "Trigger: Send enriched data")
                
                forged := sw.AddFromMsgToForged( line, sw.ForgeFromKey(k), false )
                if len(forged) > 0 {
                    sw.outEngineQueue <- forged
                }
                
            } else {
                sw.FillKeyMsgData(k, line.MsgData)
                
                if sw.waitForTrigger == false && sw.sendByInterval == false {
                    ComLogger.Write("V", "Live enrichment")
                    
                    // string factory + outEngineQueue !!!
                    forged := sw.AddFromMsgToForged( line, sw.ForgeFromKey(k), false )
                    if len(forged) > 0 {
                        sw.outEngineQueue <- forged
                    }
                } else {
                    ComLogger.Write("V", "Internal enrichment only")
                }
            }
            
            sw.SetupRoutines()
        
        default:
            sw.SetupRoutines()
            time.Sleep(100 * time.Millisecond)
        }
    }
    
    defer sw.CloseDatabase()
}

func (sw *SqliteWorker) SetupRoutines() {
    
    if sw.dmp == true && sw.sendByInterval == true {
        sw.dmp = false
        
        tickFlushing := time.Tick(time.Duration(sw.DMPIVAL) * time.Second)
        
        go func(sw *SqliteWorker) {
            for range tickFlushing {
                sw.RegularFlushing()
            }
        }(sw)
    }
    
    if sw.gb == true {
        sw.gb = false
        
        tickGarbageCollect := time.Tick(60 * time.Second)
        
        go func(sw *SqliteWorker) {
            for range tickGarbageCollect {
                sw.GarbageCollector()
            }
        }(sw)
    }
    
    t := time.Now();
    
    if t.Hour() == 2 && t.Minute() == 30 && t.Second() == 00 && sw.vac == true {
        sw.vac = false
        sw.VacuumCleaner()
    } else if t.Hour() == 2 && t.Minute() == 30 && t.Second() == 01 && sw.vac == false {
        sw.vac = true
    }
    
    if t.Minute() == 00 && t.Second() == 00 && sw.vac == true {
        sw.vac = false
        sw.VacuumCleanerIncremental()
    } else if t.Minute() == 00 && t.Second() == 01 && sw.vac == false {
        sw.vac = true
    }
}

func (sw *SqliteWorker) RegularFlushing() {
    var dump []FullMessage = sw.SelectAllByDelay(sw.DMPIVAL)
    
    for _, v := range dump {
        ComLogger.Write("V", fmt.Sprintf("Dumping event: %#v", v))
        
        sw.outEngineQueue <- sw.AddFromFullToForged( v, sw.ForgeFromMsgData(v.Mapped))
        
        k := Key{}
        if len(sw.keyStruct) > 0 {
            for _, w := range sw.keyStruct {
                if w == "id" {
                    k[w] = v.Id
                } else if w == "hostname" {
                    k[w] = v.Hostname
                } else {
                    k[w] = v.Mapped[w]
                }
            }
        } else {
            k["id"] = v.Id
        }
        
        sw.Push(k, "emitted", "1")
    }
}

func (sw *SqliteWorker) VacuumCleaner() {
    var err error
	sqlStmt := "VACUUM;"
    
    ComLogger.Write("D", sqlStmt)
    
	_, err = sw.DbObject.Exec(sqlStmt)
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
}

func (sw *SqliteWorker) VacuumCleanerIncremental() {    
    var err error
	sqlStmt := "PRAGMA incremental_vacuum(1000);"
    
    ComLogger.Write("D", sqlStmt)
    
	_, err = sw.DbObject.Exec(sqlStmt)
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
}

func (sw *SqliteWorker) GarbageCollector() {    
    var err error
	var sqlStmt strings.Builder
    
	sqlStmt.WriteString("DELETE FROM `")
	sqlStmt.WriteString(sw.TABLENAME)
	sqlStmt.WriteString("` WHERE ((strftime('%s', CURRENT_TIMESTAMP) - strftime('%s',`")
    if sw.sendByInterval {
        sqlStmt.WriteString("updated_at")
    } else {
        sqlStmt.WriteString("created_at")
    }
    sqlStmt.WriteString("`)) > ")
    sqlStmt.WriteString(fmt.Sprintf("%d", sw.GBTG))
    sqlStmt.WriteString(") AND `emitted` ")
    if sw.sendByInterval == true {
        sqlStmt.WriteString("!=")
    } else {
        sqlStmt.WriteString("=")
    }
    sqlStmt.WriteString(" 0;")
    
    ComLogger.Write("D", sqlStmt.String())
    
    tx, err := sw.DbObject.Begin()
    if err != nil {
        ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
    }
    
	_, err = tx.Exec(sqlStmt.String())
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
    
    if err := tx.Commit(); err != nil {
        _ = tx.Rollback()
        ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
    }
}

func (sw *SqliteWorker) CheckIfTableExists() bool {
    var retVal bool = false
    var err error
    var stmt *sql.Stmt
	var count int

	stmt, err = sw.DbObject.Prepare("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?;")
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
	defer stmt.Close()
    
	err = stmt.QueryRow(sw.TABLENAME).Scan(&count)
	if err != nil {
		ComLogger.Write("E", fmt.Sprintf("%s", err.Error()))
	}
    
    if count == 1 {
        retVal = true
    }
    
	return retVal
}

func (sw *SqliteWorker) Quit() {
	sw.CloseDatabase()
    ComLogger.Write("I", "Sqlite Engine quit")
}	