package token

type TokenType string

var keywords = map[string]TokenType{
	"==>":    FUNCTION,
	"$":      LET,
	"good":   TRUE,
	"bad":    FALSE,
	"if":     IF,
	"else":   ELSE,
	"return": RETURN,
	"while":  WHILE,
}

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}

	return IDENT
}

type Token struct {
	Type    TokenType
	Literal string
}

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"
	IDENT   = "IDENT"
	INT     = "INT"
	STRING  = "STRING"

	ASSIGN   = "="
	PLUS     = "+"
	MINUS    = "-"
	BANG     = "!"
	ASTERISK = "*"
	SLASH    = "/"

	COMMA     = ","
	SEMICOLON = ";"
	COLON     = ":"

	LPAREN = "("
	RPAREN = ")"

	LBRACE = "{"
	RBRACE = "}"

	LBRACKET = "["
	RBRACKET = "]"

	FUNCTION = "FUNCTION"
	LET      = "LET"
	TRUE     = "TRUE"
	FALSE    = "FALSE"
	IF       = "IF"
	ELSE     = "ELSE"
	RETURN   = "RETURN"
	WHILE    = "WHILE"

	EQUAL     = "=="
	DIFFERENT = "!="
	LESST     = "<"
	MORET     = ">"
)
