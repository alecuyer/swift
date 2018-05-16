package main

import (
	"flag"
	"github.com/jmhodges/levigo"
	"log"
	"os"
)

func main() {
	var dbPath = flag.String("dbPath", "", "LevelDB database path")
	flag.Parse()

	if os.Getuid() == 0 {
		log.Fatal("do not run as root")
	}

	opts := levigo.NewOptions()
	err := levigo.RepairDatabase(*dbPath, opts)
	if err != nil {
		log.Print(err)
	}
	log.Print("finished")
}
