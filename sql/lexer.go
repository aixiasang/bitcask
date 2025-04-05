package sql

import (
	"fmt"
	"strings"
)

// TokenType represents the type of a token
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenIdentifier
	TokenString
	TokenNumber
	TokenKeyword
	TokenOperator
	TokenComma
	TokenSemicolon
	TokenLeftParen
	TokenRightParen
	TokenEquals
	TokenAsterisk
)

// Token represents a lexical token
type Token struct {
	Type      TokenType
	Value     string
	Line      int
	Column    int
	IsKeyword bool
}

// Keywords is a map of SQL keywords
var Keywords = map[string]bool{
	"CREATE":  true,
	"TABLE":   true,
	"INSERT":  true,
	"INTO":    true,
	"VALUES":  true,
	"SELECT":  true,
	"FROM":    true,
	"WHERE":   true,
	"AND":     true,
	"OR":      true,
	"NOT":     true,
	"NULL":    true,
	"INTEGER": true,
	"TEXT":    true,
	"VARCHAR": true,
	"CHAR":    true,
	"PRIMARY": true,
	"KEY":     true,
	"DELETE":  true,
	"UPDATE":  true,
	"SET":     true,
	"DROP":    true,
}

// Lexer is responsible for tokenizing SQL statements
type Lexer struct {
	input   string
	pos     int
	readPos int
	ch      byte
	line    int
	column  int
}

// NewLexer creates a new Lexer
func NewLexer(input string) *Lexer {
	l := &Lexer{
		input:  input,
		line:   1,
		column: 0,
	}
	l.readChar()
	return l
}

// readChar reads the next character and advances the position
func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
	l.column++

	// Handle newlines
	if l.ch == '\n' {
		l.line++
		l.column = 0
	}
}

// peekChar returns the next character without advancing the position
func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

// NextToken returns the next token
func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	// Store current position info for the token
	tok.Line = l.line
	tok.Column = l.column

	switch l.ch {
	case 0:
		tok = Token{Type: TokenEOF, Value: ""}
	case '(':
		tok = Token{Type: TokenLeftParen, Value: string(l.ch)}
	case ')':
		tok = Token{Type: TokenRightParen, Value: string(l.ch)}
	case ',':
		tok = Token{Type: TokenComma, Value: string(l.ch)}
	case ';':
		tok = Token{Type: TokenSemicolon, Value: string(l.ch)}
	case '=':
		tok = Token{Type: TokenEquals, Value: string(l.ch)}
	case '*':
		tok = Token{Type: TokenAsterisk, Value: string(l.ch)}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenOperator, Value: ">="}
		} else {
			tok = Token{Type: TokenOperator, Value: ">"}
		}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenOperator, Value: "<="}
		} else {
			tok = Token{Type: TokenOperator, Value: "<"}
		}
	case '\'', '"':
		quote := l.ch
		tok.Type = TokenString
		tok.Value = l.readString(quote)
	default:
		if isLetter(l.ch) {
			tok.Value = l.readIdentifier()
			upper := strings.ToUpper(tok.Value)
			if Keywords[upper] {
				tok.Type = TokenKeyword
				tok.Value = upper
				tok.IsKeyword = true
			} else {
				tok.Type = TokenIdentifier
			}
			return tok
		} else if isDigit(l.ch) {
			tok.Type = TokenNumber
			tok.Value = l.readNumber()
			return tok
		} else {
			tok = Token{Type: TokenOperator, Value: string(l.ch)}
		}
	}

	l.readChar()
	return tok
}

// readIdentifier reads an identifier
func (l *Lexer) readIdentifier() string {
	startPos := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[startPos:l.pos]
}

// readNumber reads a number
func (l *Lexer) readNumber() string {
	startPos := l.pos
	for isDigit(l.ch) {
		l.readChar()
	}
	return l.input[startPos:l.pos]
}

// readString reads a string enclosed in quotes
func (l *Lexer) readString(quote byte) string {
	startPos := l.pos + 1 // Skip the opening quote
	for {
		l.readChar()
		if l.ch == quote || l.ch == 0 {
			break
		}
	}
	return l.input[startPos:l.pos]
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// TokenizeSQL tokenizes an SQL statement into tokens
func TokenizeSQL(sql string) ([]Token, error) {
	lexer := NewLexer(sql)
	tokens := []Token{}

	for {
		token := lexer.NextToken()
		tokens = append(tokens, token)
		if token.Type == TokenEOF {
			break
		}
	}

	return tokens, nil
}

// isLetter checks if a character is a letter
func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z'
}

// isDigit checks if a character is a digit
func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

// TokenToString converts a token to a debug string
func TokenToString(token Token) string {
	switch token.Type {
	case TokenEOF:
		return "EOF"
	case TokenIdentifier:
		return fmt.Sprintf("IDENTIFIER(%s)", token.Value)
	case TokenString:
		return fmt.Sprintf("STRING(%s)", token.Value)
	case TokenNumber:
		return fmt.Sprintf("NUMBER(%s)", token.Value)
	case TokenKeyword:
		return fmt.Sprintf("KEYWORD(%s)", token.Value)
	case TokenOperator:
		return fmt.Sprintf("OPERATOR(%s)", token.Value)
	case TokenComma:
		return "COMMA"
	case TokenSemicolon:
		return "SEMICOLON"
	case TokenLeftParen:
		return "LEFT_PAREN"
	case TokenRightParen:
		return "RIGHT_PAREN"
	case TokenEquals:
		return "EQUALS"
	case TokenAsterisk:
		return "ASTERISK"
	default:
		return fmt.Sprintf("UNKNOWN(%s)", token.Value)
	}
}
