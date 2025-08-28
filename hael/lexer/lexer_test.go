package lexer

import (
	"fmt"
	"hael/token"
	"testing"
)

func TestNextToken(t *testing.T) {
	input := `$five = 5;

	$ten = 10;

	$add ==> (x, y) {
	    x + y;
	};

	$result = add(five, ten);

	!-/*5;

	5 < 10 > 5;
	

	if (5 < 10) {
		return good;
	} else {
	    return bad; 
	}

	10 == 10;

	10 != 9;
	"foobar"
	"foo bar"
	[1,2];
	{"foo": "bar"}

	while (i < 20) {
		print(i)
		$i = i + 1
	}
	`

	tests := []struct {
		expectedType    token.TokenType
		expectedLiteral string
	}{
		{token.LET, "$"},
		{token.IDENT, "five"},
		{token.ASSIGN, "="},
		{token.INT, "5"},
		{token.SEMICOLON, ";"},
		{token.LET, "$"},
		{token.IDENT, "ten"},
		{token.ASSIGN, "="},
		{token.INT, "10"},
		{token.SEMICOLON, ";"},
		{token.LET, "$"},
		{token.IDENT, "add"},
		// {token.ASSIGN, "="},
		{token.FUNCTION, "==>"},
		{token.LPAREN, "("},
		{token.IDENT, "x"},
		{token.COMMA, ","},
		{token.IDENT, "y"},
		{token.RPAREN, ")"},
		{token.LBRACE, "{"},
		{token.IDENT, "x"},
		{token.PLUS, "+"},
		{token.IDENT, "y"},
		{token.SEMICOLON, ";"},
		{token.RBRACE, "}"},
		{token.SEMICOLON, ";"},
		{token.LET, "$"},
		{token.IDENT, "result"},
		{token.ASSIGN, "="},
		{token.IDENT, "add"},
		{token.LPAREN, "("},
		{token.IDENT, "five"},
		{token.COMMA, ","},
		{token.IDENT, "ten"},
		{token.RPAREN, ")"},
		{token.SEMICOLON, ";"},
		{token.BANG, "!"},
		{token.MINUS, "-"},
		{token.SLASH, "/"},
		{token.ASTERISK, "*"},
		{token.INT, "5"},
		{token.SEMICOLON, ";"},

		{token.INT, "5"},
		{token.LESST, "<"},
		{token.INT, "10"},
		{token.MORET, ">"},
		{token.INT, "5"},
		{token.SEMICOLON, ";"},

		{token.IF, "if"},
		{token.LPAREN, "("},
		{token.INT, "5"},
		{token.LESST, "<"},
		{token.INT, "10"},
		{token.RPAREN, ")"},
		{token.LBRACE, "{"},
		{token.RETURN, "return"},
		{token.TRUE, "good"},
		{token.SEMICOLON, ";"},
		{token.RBRACE, "}"},
		{token.ELSE, "else"},
		{token.LBRACE, "{"},
		{token.RETURN, "return"},
		{token.FALSE, "bad"},
		{token.SEMICOLON, ";"},
		{token.RBRACE, "}"},

		{token.INT, "10"},
		{token.EQUAL, "=="},
		{token.INT, "10"},
		{token.SEMICOLON, ";"},

		{token.INT, "10"},
		{token.DIFFERENT, "!="},
		{token.INT, "9"},
		{token.SEMICOLON, ";"},
		{token.STRING, "foobar"},
		{token.STRING, "foo bar"},

		{token.LBRACKET, "["},
		{token.INT, "1"},
		{token.COMMA, ","},
		{token.INT, "2"},
		{token.RBRACKET, "]"},
		{token.SEMICOLON, ";"},

		{token.LBRACE, "{"},
		{token.STRING, "foo"},
		{token.COLON, ":"},
		{token.STRING, "bar"},
		{token.RBRACE, "}"},

		{token.EOF, ""},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()

		fmt.Println(tok)

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%q, got=%q", i, tt.expectedType, tok.Type)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q", i, tt.expectedLiteral, tok.Literal)
		}
	}
}
