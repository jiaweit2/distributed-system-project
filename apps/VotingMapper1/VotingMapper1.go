package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func calculateVote(line string, result map[string][]int) {
	votes := strings.Split(line, ",")
	for i := 0; i < len(votes); i++ {
		for j := i + 1; j < len(votes); j++ {
			if votes[i] < votes[j] {
				key := votes[i] + "," + votes[j]
				if _, ok := result[key]; ok {
					result[key][0]++
				} else {
					result[key] = []int{1, 0}
				}
			} else {
				key := votes[j] + "," + votes[i]
				if _, ok := result[key]; ok {
					result[key][1]++
				} else {
					result[key] = []int{0, 1}
				}
			}
		}
	}
}

func main() {

	args := os.Args
	result := make(map[string][]int)

	if len(args) < 2 {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			calculateVote(scanner.Text(), result)
		}
		for key, val := range result {
			s := key + "\t" + strconv.Itoa(val[0]) + ";" + strconv.Itoa(val[1])
			fmt.Println(s)
		}
	} else {
		input := args[1]
		outPrefix := args[2]

		file, err := os.Open(input)
		if err != nil {
			log.Fatal(err)
		}

		defer file.Close()
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			calculateVote(scanner.Text(), result)
		}

		var home, _ = os.UserHomeDir()
		for key, val := range result {
			outFile, err := os.Create(home + "/local/" + outPrefix + "_" + key)
			if err != nil {
				log.Fatal(err)
			}
			_, err = outFile.WriteString(strconv.Itoa(val[0]) + ";" + strconv.Itoa(val[1]) + "\n")
			if err != nil {
				log.Fatal(err)
			}
			outFile.Close()
			fmt.Println(outPrefix + "_" + key)
		}
	}
}
