package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func split2Half(input string, delimiter string) (string, string) {
	tempArr := strings.Split(input, delimiter)
	return tempArr[0], tempArr[1]
}

func main() {

	args := os.Args
	Avote := 0
	Bvote := 0
	if len(args) < 2 {
		var current_key, key string
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Text()
			key_loc, val := split2Half(line, "\t")
			key = key_loc
			if key != current_key {
				if current_key != "" {
					a, b := split2Half(current_key, ",")
					winner := a + "," + b
					if Avote < Bvote {
						winner = b + "," + a
					}
					fmt.Println("MR1_res\t" + winner)
					Avote = 0
					Bvote = 0
				}
				current_key = key
			}
			a, b := split2Half(val, ";")
			avote, _ := strconv.Atoi(a)
			bvote, _ := strconv.Atoi(b)
			Avote += avote
			Bvote += bvote
		}
		if current_key == key && key != "" {
			a, b := split2Half(current_key, ",")
			winner := a + "," + b
			if Avote < Bvote {
				winner = b + "," + a
			}
			fmt.Println("MR1_res\t" + winner)
		}
	} else {
		input := args[1]
		outFile := args[2]
		tempArr := strings.Split(input, "_")
		key := tempArr[len(tempArr)-1]
		A, B := split2Half(key, ",")

		file, err := os.Open(input)
		if err != nil {
			log.Fatal(err)
		}

		defer file.Close()
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			a, b := split2Half(scanner.Text(), ";")
			avote, _ := strconv.Atoi(a)
			bvote, _ := strconv.Atoi(b)
			Avote += avote
			Bvote += bvote
		}

		winner := A + "," + B
		if Avote < Bvote {
			winner = B + "," + A
		}

		var home, _ = os.UserHomeDir()
		f, err := os.OpenFile(home+"/local/"+outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		_, err = f.WriteString(winner + "\n")
		if err != nil {
			log.Fatal(err)
		}
		f.Close()
	}

}
