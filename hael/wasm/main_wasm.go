package main

import (
	"hael/evaluator"
	"hael/lexer"
	"hael/object"
	"hael/parser"
	"runtime"
	"strings"
	"syscall/js"
)

func evaluateCode(this js.Value, args []js.Value) interface{} {
	if len(args) != 1 {
		return "Error: expected 1 argument"
	}
	code := args[0].String()
	env := object.NewEnvironment()
	if runtime.GOOS == "js" {
		evaluator.InitWASMBuiltins(env)
	} else {
		evaluator.InitBuiltins(env)
	}
	l := lexer.New(code)
	p := parser.New(l)
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		errors := strings.Join(p.Errors(), "\n")
		return "Parser errors:\n" + errors
	}
	result := evaluator.Eval(program, env)
	return result.Inspect()
}

func main() {
	js.Global().Set("evaluateCode", js.FuncOf(evaluateCode))
	select {}
}
