package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func split2Half(input string, delimiter string) (string, string) {
	tempArr := strings.Split(input, delimiter)
	return tempArr[0], tempArr[1]
}

func main() {

	args := os.Args

	if len(args) < 2 {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
	} else {
		input := args[1]
		outPrefix := args[2]

		var home, _ = os.UserHomeDir()
		e := os.Rename(input, home+"/local/"+outPrefix+"_1")
		if e != nil {
			log.Fatal(e)
		}
		fmt.Println(outPrefix + "_1")
	}

}
