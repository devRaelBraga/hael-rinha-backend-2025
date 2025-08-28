package main

import (
	"fmt"
	"hael/evaluator"
	"hael/lexer"
	"hael/object"
	"hael/parser"
	"hael/repl"
	"io"
	"os"
	"path/filepath"
)

func main() {

	if len(os.Args) == 1 {
		fmt.Printf("\n      __             __\n   .-'.'     .-.     '.'-.\n .'.((      ( ^ `>     )).'. \n/`'- \\'._____\\ (_____.'/ -'`\\\n|-''`.'------' '------'.`''-|\n|.-'`.'.'.`/ | | \\`.'.'.`'-.|\n \\ .' . /  | | | |  \\ . '. /\n  '._. :  _|_| |_|_  : ._.'\n     ````` /T\"Y\"T\\ `````\n          / | | | \\\n         `'`'`'`'`'`\n\n")

		fmt.Printf("Hello!\nThis is the Hael programming language!\n")

		fmt.Printf("Feel free to type in commands\n\n")

		repl.Start(os.Stdin, os.Stdout)

		return
	}

	if len(os.Args) < 2 {
		fmt.Println("Erro: Nenhum comando fornecido.")
		fmt.Println("Uso: hael <comando> <caminho_do_arquivo>")
		os.Exit(1)
	}

	command := os.Args[1]

	if len(os.Args) < 3 {
		fmt.Println("Erro: O caminho do arquivo é obrigatório.")
		fmt.Println("Uso: hael <comando> <caminho_do_arquivo>")
		os.Exit(1)
	}

	filePath := os.Args[2]

	absPath, err := filepath.Abs(filePath)
	if err != nil {
		fmt.Printf("Erro ao resolver o caminho do arquivo: %v\n", err)
		os.Exit(1)
	}

	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		fmt.Printf("Erro: O arquivo %s não existe.\n", absPath)
		os.Exit(1)
	}

	switch command {
	case "run":
		evalAndRun(filePath)
	default:
		fmt.Printf("Comando desconhecido: %s\n", command)
		fmt.Println("Comandos disponíveis: run")
		os.Exit(1)
	}
}

func evalAndRun(path string) {
	content, err := os.ReadFile(path)
	out := os.Stdout

	if err != nil {
		fmt.Println("Erro ao ler o arquivo:", err)
		return
	}

	input := string(content)

	l := lexer.New(input)
	p := parser.New(l)

	program := p.ParseProgram()
	env := object.NewEnvironment()
	env.Set("__current_file__", &object.String{Value: path})
	evaluator.InitBuiltins(env)
	if len(p.Errors()) != 0 {
		printParserErrors(out, p.Errors())
	}

	evaluated := evaluator.Eval(program, env)

	if evaluated != nil {
		io.WriteString(out, ">> ")
		io.WriteString(out, evaluated.Inspect())
		io.WriteString(out, "\n")
	}
}

func printParserErrors(out io.Writer, errors []string) {
	for _, msg := range errors {
		io.WriteString(out, "\t"+msg+"\n")
	}
}
