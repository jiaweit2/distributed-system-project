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

func getWinner(result map[string]int) string {
	maxCnt := 0
	var winner string
	for cand, cnt := range result {
		if cnt > maxCnt {
			winner = cand
			maxCnt = cnt
		}
	}
	return winner
}

func main() {

	args := os.Args
	result := make(map[string]int)

	if len(args) < 2 {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Text()
			_, val := split2Half(line, "\t")
			pairs := strings.Split(val, ",")
			result[pairs[0]]++
		}
		winner := getWinner(result)
		fmt.Println("Final_result\t" + winner)

	} else {
		input := args[1]
		outFile := args[2]

		file, err := os.Open(input)
		if err != nil {
			log.Fatal(err)
		}

		defer file.Close()
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			pairs := strings.Split(scanner.Text(), ",")
			result[pairs[0]]++
		}

		winner := getWinner(result)

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
