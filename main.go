package main

// build CentOS Appliance : env GOOS=linux GOARCH=amd64 go build

import (
    "flag"
)

func main() {
    var configFile string
    flag.StringVar(&configFile, "config", "settings.json", "The targeted Configuration File (from the storage folder).")
    flag.Parse()
    
    preprocessor := Preprocessor{}
    preprocessor.New(configFile)
    preprocessor.Run()
}
