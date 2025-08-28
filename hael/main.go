package main

import (
	"fmt"
	"hael/repl"
	"os"
	"os/user"
)

func main() {
	user, err := user.Current()

	if err != nil {
		panic(err)
	}
	fmt.Printf("\n      __             __\n   .-'.'     .-.     '.'-.\n .'.((      ( ^ `>     )).'. \n/`'- \\'._____\\ (_____.'/ -'`\\\n|-''`.'------' '------'.`''-|\n|.-'`.'.'.`/ | | \\`.'.'.`'-.|\n \\ .' . /  | | | |  \\ . '. /\n  '._. :  _|_| |_|_  : ._.'\n     ````` /T\"Y\"T\\ `````\n          / | | | \\\n         `'`'`'`'`'`\n\n")

	fmt.Printf("Hello %s!\nThis is the Hael programming language!\n", user.Username)

	fmt.Printf("Feel free to type in commands\n\n")

	repl.Start(os.Stdin, os.Stdout)
}
