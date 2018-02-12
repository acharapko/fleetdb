package main

import (
	"flag"
	"bufio"
	"math/rand"

	"github.com/acharapko/fleetdb"
	"fmt"
	"os"
	"strings"
	"strconv"
)

// db implements db.DB interface for Client
type consoleClient struct {
	client *fleetdb.Client
}

func main() {
	flag.Parse()

	id := fleetdb.GetID()

	var config fleetdb.Config
	config = fleetdb.NewConfig(id)
	cc := new(consoleClient)
	cc.client = fleetdb.NewClient(config)

	fmt.Printf("Welcome to fleetdb Console. Home Node is %v\n", id)
	cc.RunConsole()
}


const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func (cc *consoleClient) generateRandVal(n int) []byte {
	bts := make([]byte, n)
	for i := range bts {
		bts[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return bts
}


func (cc *consoleClient) putKeys(fromK, toK, n int) {
	for k := fromK; k <= toK; k++ {
		v := cc.generateRandVal(n)
		cc.client.Put(fleetdb.Key([]byte(strconv.Itoa(k))), v)
	}
}

func (cc *consoleClient) RunConsole() {
	fmt.Println("Starting Console")
	keepRunning := true
	reader := bufio.NewReader(os.Stdin)

	for keepRunning {
		fmt.Print(">> ")

		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		if text == "exit" {
			keepRunning = false
		} else {
			//we have non exit command
			//this is a simple DB console, so we have only 3 commands
			//put, delete and get
			//put key value
			//delete key
			//get key
			//----
			//so no fancy parsing is needed, just read the first word for command, second for key (no spaces allowed)
			//and the rest is value (if any)

			parts := strings.Split(text, " ")
			if len(parts) > 1 {
				keyStr := parts[1]
				switch parts[0] {
				case "populate":
					if len(parts) >= 4 {
						fromK, _ := strconv.Atoi(keyStr)
						toK, _ := strconv.Atoi(parts[2])
						n, _ := strconv.Atoi(parts[3])
						cc.putKeys(fromK, toK, n)
					} else {
						fmt.Println("populate command error. Must be in format: populate fromKey toKey valSize")
					}
				case "put":
					if len(parts) >= 3 {
						val := strings.Join(parts[2:len(parts)], " ")
						fmt.Println(keyStr + " -> " + val)
						cc.client.Put(fleetdb.Key([]byte(keyStr)), []byte(val))
					} else {
						fmt.Println("Put command error. Must be in format: put key value")
					}
				case "puttx":
					if len(parts) >= 3 {
						numK := (len(parts) - 1) / 2

						keys := make([]fleetdb.Key, numK)
						vals := make([]fleetdb.Value, numK)
						for i := 1; i < len(parts); i+=2 {
							keyStr := parts[i]
							val := parts[i+1]
							fmt.Println(keyStr + " -> " + val)
							keys[(i-1) / 2] = fleetdb.Key([]byte(keyStr))
							vals[(i-1) / 2] = fleetdb.Value([]byte(val))
						}
						cc.client.PutTx(keys, vals)
					} else {
						fmt.Println("Puttx command error. Must be in format: puttx key value ...")
					}
				case "putget":
					if len(parts) >= 4 {
						val := parts[2]
						key2 := parts[3]
						fmt.Println(keyStr + " -> " + val)
						keys := make([]fleetdb.Key, 2)
						vals := make([]fleetdb.Value, 1)
						keys[0] = fleetdb.Key([]byte(keyStr))
						keys[1] = fleetdb.Key([]byte(key2))
						cc.client.PrepTx()
						vals[0] = fleetdb.Value([]byte(val))
						cc.client.AddTxPut(keys[0], vals[0])
						cc.client.AddTxGet(keys[1])
						cc.client.SendTX()
					} else {
						fmt.Println("Puttx command error. Must be in format: puttx key value ...")
					}
				case "delete":
					cc.client.Delete(fleetdb.Key([]byte(keyStr)))
				case "get":
					retVal := cc.client.Get(fleetdb.Key([]byte(keyStr)))
					retValStr := string(retVal)
					fmt.Println(retValStr)
				}
			} else {
				fmt.Println("Incorrect syntax for " + parts[0] + " command: key is required")
			}
		}
	}
}