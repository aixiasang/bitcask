package sql

import (
	"errors"
	"fmt"
	"strings"
)

// Statement types
type StatementType string

const (
	CreateTableStmt StatementType = "CREATE_TABLE"
	InsertStmt      StatementType = "INSERT"
	SelectStmt      StatementType = "SELECT"
	DeleteStmt      StatementType = "DELETE"
	UpdateStmt      StatementType = "UPDATE"
	DropTableStmt   StatementType = "DROP_TABLE"
)

// Column definition for table schema
type ColumnDef struct {
	Name       string
	Type       string
	PrimaryKey bool
}

// AST node interface
type Node interface {
	Type() StatementType
	String() string
}

// CreateTable statement AST node
type CreateTableNode struct {
	TableName string
	Columns   []ColumnDef
}

func (n CreateTableNode) Type() StatementType {
	return CreateTableStmt
}

func (n CreateTableNode) String() string {
	cols := make([]string, len(n.Columns))
	for i, col := range n.Columns {
		pkStr := ""
		if col.PrimaryKey {
			pkStr = " PRIMARY KEY"
		}
		cols[i] = fmt.Sprintf("%s %s%s", col.Name, col.Type, pkStr)
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", n.TableName, strings.Join(cols, ", "))
}

// Insert statement AST node
type InsertNode struct {
	TableName string
	Columns   []string
	Values    [][]string
}

func (n InsertNode) Type() StatementType {
	return InsertStmt
}

func (n InsertNode) String() string {
	cols := strings.Join(n.Columns, ", ")
	var valueStrs []string
	for _, row := range n.Values {
		valueStrs = append(valueStrs, "("+strings.Join(row, ", ")+")")
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", n.TableName, cols, strings.Join(valueStrs, ", "))
}

// Condition represents a WHERE condition
type Condition struct {
	Left     string
	Operator string
	Right    string
}

// Select statement AST node
type SelectNode struct {
	Columns     []string
	TableName   string
	Conditions  []Condition
	WildcardAll bool
}

func (n SelectNode) Type() StatementType {
	return SelectStmt
}

func (n SelectNode) String() string {
	var colStr string
	if n.WildcardAll {
		colStr = "*"
	} else {
		colStr = strings.Join(n.Columns, ", ")
	}

	whereClause := ""
	if len(n.Conditions) > 0 {
		var condStrs []string
		for _, cond := range n.Conditions {
			condStrs = append(condStrs, fmt.Sprintf("%s %s %s", cond.Left, cond.Operator, cond.Right))
		}
		whereClause = " WHERE " + strings.Join(condStrs, " AND ")
	}

	return fmt.Sprintf("SELECT %s FROM %s%s", colStr, n.TableName, whereClause)
}

// Delete statement AST node
type DeleteNode struct {
	TableName  string
	Conditions []Condition
}

func (n DeleteNode) Type() StatementType {
	return DeleteStmt
}

func (n DeleteNode) String() string {
	whereClause := ""
	if len(n.Conditions) > 0 {
		var condStrs []string
		for _, cond := range n.Conditions {
			condStrs = append(condStrs, fmt.Sprintf("%s %s %s", cond.Left, cond.Operator, cond.Right))
		}
		whereClause = " WHERE " + strings.Join(condStrs, " AND ")
	}

	return fmt.Sprintf("DELETE FROM %s%s", n.TableName, whereClause)
}

// Update statement AST node
type UpdateNode struct {
	TableName  string
	Columns    []string
	Values     []string
	Conditions []Condition
}

func (n UpdateNode) Type() StatementType {
	return UpdateStmt
}

func (n UpdateNode) String() string {
	// Build SET clause
	var setStrings []string
	for i := 0; i < len(n.Columns); i++ {
		setStrings = append(setStrings, fmt.Sprintf("%s = %s", n.Columns[i], n.Values[i]))
	}
	setClause := strings.Join(setStrings, ", ")

	// Build WHERE clause
	whereClause := ""
	if len(n.Conditions) > 0 {
		var condStrs []string
		for _, cond := range n.Conditions {
			condStrs = append(condStrs, fmt.Sprintf("%s %s %s", cond.Left, cond.Operator, cond.Right))
		}
		whereClause = " WHERE " + strings.Join(condStrs, " AND ")
	}

	return fmt.Sprintf("UPDATE %s SET %s%s", n.TableName, setClause, whereClause)
}

// DropTable statement AST node
type DropTableNode struct {
	TableName string
}

func (n DropTableNode) Type() StatementType {
	return DropTableStmt
}

func (n DropTableNode) String() string {
	return fmt.Sprintf("DROP TABLE %s", n.TableName)
}

// Parser is responsible for parsing SQL tokens into an AST
type Parser struct {
	tokens  []Token
	currPos int
}

// NewParser creates a new parser from tokens
func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens:  tokens,
		currPos: 0,
	}
}

// Parse parses the tokens and returns an AST
func Parse(sql string) (Node, error) {
	tokens, err := TokenizeSQL(sql)
	if err != nil {
		return nil, err
	}

	parser := NewParser(tokens)
	return parser.parseStatement()
}

// parseStatement parses a SQL statement
func (p *Parser) parseStatement() (Node, error) {
	if p.currPos >= len(p.tokens) {
		return nil, errors.New("unexpected end of input")
	}

	token := p.current()
	if token.Type != TokenKeyword {
		return nil, fmt.Errorf("expected keyword, got %s", TokenToString(token))
	}

	switch token.Value {
	case "CREATE":
		return p.parseCreateTable()
	case "INSERT":
		return p.parseInsert()
	case "SELECT":
		return p.parseSelect()
	case "DELETE":
		return p.parseDelete()
	case "UPDATE":
		return p.parseUpdate()
	case "DROP":
		return p.parseDropTable()
	default:
		return nil, fmt.Errorf("unsupported statement type: %s", token.Value)
	}
}

// parseCreateTable parses a CREATE TABLE statement
func (p *Parser) parseCreateTable() (Node, error) {
	// Verify "CREATE"
	if !p.expectKeyword("CREATE") {
		return nil, errors.New("expected CREATE keyword")
	}
	p.advance()

	// Verify "TABLE"
	if !p.expectKeyword("TABLE") {
		return nil, errors.New("expected TABLE keyword")
	}
	p.advance()

	// Get table name
	if !p.expectType(TokenIdentifier) {
		return nil, errors.New("expected table name")
	}
	tableName := p.current().Value
	p.advance()

	// Verify "("
	if !p.expectType(TokenLeftParen) {
		return nil, errors.New("expected ( after table name")
	}
	p.advance()

	// Parse columns
	columns, err := p.parseColumnDefs()
	if err != nil {
		return nil, err
	}

	// Verify ")"
	if !p.expectType(TokenRightParen) {
		return nil, errors.New("expected ) after column definitions")
	}
	p.advance()

	return CreateTableNode{
		TableName: tableName,
		Columns:   columns,
	}, nil
}

// parseColumnDefs parses column definitions
func (p *Parser) parseColumnDefs() ([]ColumnDef, error) {
	columns := []ColumnDef{}

	for {
		// Get column name
		if !p.expectType(TokenIdentifier) {
			return nil, errors.New("expected column name")
		}
		colName := p.current().Value
		p.advance()

		// Get column type
		if !p.expectType(TokenIdentifier) && !p.expectType(TokenKeyword) {
			return nil, errors.New("expected column type")
		}
		colType := p.current().Value
		p.advance()

		// Check for PRIMARY KEY constraint
		isPrimaryKey := false
		if p.currPos < len(p.tokens) && p.current().Type == TokenKeyword && p.current().Value == "PRIMARY" {
			p.advance()
			if !p.expectKeyword("KEY") {
				return nil, errors.New("expected KEY after PRIMARY")
			}
			p.advance()
			isPrimaryKey = true
		}

		columns = append(columns, ColumnDef{
			Name:       colName,
			Type:       colType,
			PrimaryKey: isPrimaryKey,
		})

		// Check if there are more columns
		if p.currPos >= len(p.tokens) || p.current().Type != TokenComma {
			break
		}
		p.advance() // Skip comma
	}

	return columns, nil
}

// parseInsert parses an INSERT statement
func (p *Parser) parseInsert() (Node, error) {
	// Verify "INSERT"
	if !p.expectKeyword("INSERT") {
		return nil, errors.New("expected INSERT keyword")
	}
	p.advance()

	// Verify "INTO"
	if !p.expectKeyword("INTO") {
		return nil, errors.New("expected INTO keyword")
	}
	p.advance()

	// Get table name
	if !p.expectType(TokenIdentifier) {
		return nil, errors.New("expected table name")
	}
	tableName := p.current().Value
	p.advance()

	// Parse column list
	if !p.expectType(TokenLeftParen) {
		return nil, errors.New("expected ( after table name")
	}
	p.advance()

	columns := []string{}
	for {
		if !p.expectType(TokenIdentifier) {
			return nil, errors.New("expected column name")
		}
		columns = append(columns, p.current().Value)
		p.advance()

		if p.current().Type == TokenRightParen {
			break
		}

		if !p.expectType(TokenComma) {
			return nil, errors.New("expected comma between column names")
		}
		p.advance()
	}
	p.advance() // Skip )

	// Verify "VALUES"
	if !p.expectKeyword("VALUES") {
		return nil, errors.New("expected VALUES keyword")
	}
	p.advance()

	// Parse values
	values := [][]string{}
	for {
		if !p.expectType(TokenLeftParen) {
			return nil, errors.New("expected ( for values")
		}
		p.advance()

		rowValues := []string{}
		for {
			if p.current().Type == TokenString || p.current().Type == TokenNumber {
				rowValues = append(rowValues, p.current().Value)
			} else {
				return nil, errors.New("expected string or number value")
			}
			p.advance()

			if p.current().Type == TokenRightParen {
				break
			}

			if !p.expectType(TokenComma) {
				return nil, errors.New("expected comma between values")
			}
			p.advance()
		}
		p.advance() // Skip )

		values = append(values, rowValues)

		// Check if there are more value lists
		if p.currPos >= len(p.tokens) || p.current().Type != TokenComma {
			break
		}
		p.advance() // Skip comma
	}

	return InsertNode{
		TableName: tableName,
		Columns:   columns,
		Values:    values,
	}, nil
}

// parseSelect parses a SELECT statement
func (p *Parser) parseSelect() (Node, error) {
	// Verify "SELECT"
	if !p.expectKeyword("SELECT") {
		return nil, errors.New("expected SELECT keyword")
	}
	p.advance()

	// Parse column list or *
	columns := []string{}
	wildcardAll := false

	if p.current().Type == TokenAsterisk {
		wildcardAll = true
		p.advance()
	} else {
		for {
			if !p.expectType(TokenIdentifier) {
				return nil, errors.New("expected column name")
			}
			columns = append(columns, p.current().Value)
			p.advance()

			if p.currPos >= len(p.tokens) || p.current().Type != TokenComma {
				break
			}
			p.advance() // Skip comma
		}
	}

	// Verify "FROM"
	if !p.expectKeyword("FROM") {
		return nil, errors.New("expected FROM keyword")
	}
	p.advance()

	// Get table name
	if !p.expectType(TokenIdentifier) {
		return nil, errors.New("expected table name")
	}
	tableName := p.current().Value
	p.advance()

	// Parse WHERE clause if present
	conditions := []Condition{}

	// Check if there's a WHERE clause and we haven't reached EOF
	if p.currPos < len(p.tokens) && p.current().Type == TokenKeyword && p.current().Value == "WHERE" {
		p.advance()

		// Check if we still have tokens
		if p.currPos >= len(p.tokens) {
			return nil, errors.New("unexpected end of input after WHERE")
		}

		// Get the left side of the condition
		if !p.expectType(TokenIdentifier) {
			return nil, fmt.Errorf("expected column name in WHERE clause, got %s", TokenToString(p.current()))
		}
		left := p.current().Value
		p.advance()

		// Check we still have tokens for the operator
		if p.currPos >= len(p.tokens) {
			return nil, errors.New("unexpected end of input, expected operator")
		}

		// Get the operator
		if p.current().Type != TokenEquals && p.current().Type != TokenOperator {
			return nil, fmt.Errorf("expected comparison operator in WHERE clause, got %s", TokenToString(p.current()))
		}

		var operator string
		if p.current().Type == TokenEquals {
			operator = "="
		} else {
			operator = p.current().Value
		}
		p.advance()

		// Check we still have tokens for the value
		if p.currPos >= len(p.tokens) {
			return nil, errors.New("unexpected end of input, expected value")
		}

		// Get the value
		var right string
		if p.current().Type == TokenString {
			right = p.current().Value
		} else if p.current().Type == TokenNumber {
			right = p.current().Value
		} else {
			return nil, fmt.Errorf("expected string or number value in WHERE clause, got %s", TokenToString(p.current()))
		}
		p.advance()

		// Add the condition
		conditions = append(conditions, Condition{
			Left:     left,
			Operator: operator,
			Right:    right,
		})
	}

	return SelectNode{
		Columns:     columns,
		TableName:   tableName,
		Conditions:  conditions,
		WildcardAll: wildcardAll,
	}, nil
}

// parseDelete parses a DELETE statement
func (p *Parser) parseDelete() (Node, error) {
	// Verify "DELETE"
	if !p.expectKeyword("DELETE") {
		return nil, errors.New("expected DELETE keyword")
	}
	p.advance()

	// Verify "FROM"
	if !p.expectKeyword("FROM") {
		return nil, errors.New("expected FROM keyword")
	}
	p.advance()

	// Get table name
	if !p.expectType(TokenIdentifier) {
		return nil, errors.New("expected table name")
	}
	tableName := p.current().Value
	p.advance()

	// Parse WHERE clause if present
	conditions := []Condition{}

	// Check if there's a WHERE clause and we haven't reached EOF
	if p.currPos < len(p.tokens) && p.current().Type == TokenKeyword && p.current().Value == "WHERE" {
		p.advance()

		// Check if we still have tokens
		if p.currPos >= len(p.tokens) {
			return nil, errors.New("unexpected end of input after WHERE")
		}

		// Get the left side of the condition
		if !p.expectType(TokenIdentifier) {
			return nil, fmt.Errorf("expected column name in WHERE clause, got %s", TokenToString(p.current()))
		}
		left := p.current().Value
		p.advance()

		// Check we still have tokens for the operator
		if p.currPos >= len(p.tokens) {
			return nil, errors.New("unexpected end of input, expected operator")
		}

		// Get the operator
		if p.current().Type != TokenEquals && p.current().Type != TokenOperator {
			return nil, fmt.Errorf("expected comparison operator in WHERE clause, got %s", TokenToString(p.current()))
		}

		var operator string
		if p.current().Type == TokenEquals {
			operator = "="
		} else {
			operator = p.current().Value
		}
		p.advance()

		// Check we still have tokens for the value
		if p.currPos >= len(p.tokens) {
			return nil, errors.New("unexpected end of input, expected value")
		}

		// Get the value
		var right string
		if p.current().Type == TokenString {
			right = p.current().Value
		} else if p.current().Type == TokenNumber {
			right = p.current().Value
		} else {
			return nil, fmt.Errorf("expected string or number value in WHERE clause, got %s", TokenToString(p.current()))
		}
		p.advance()

		// Add the condition
		conditions = append(conditions, Condition{
			Left:     left,
			Operator: operator,
			Right:    right,
		})
	}

	return DeleteNode{
		TableName:  tableName,
		Conditions: conditions,
	}, nil
}

// parseUpdate parses an UPDATE statement
func (p *Parser) parseUpdate() (Node, error) {
	// Verify "UPDATE"
	if !p.expectKeyword("UPDATE") {
		return nil, errors.New("expected UPDATE keyword")
	}
	p.advance()

	// Get table name
	if !p.expectType(TokenIdentifier) {
		return nil, errors.New("expected table name")
	}
	tableName := p.current().Value
	p.advance()

	// Verify "SET"
	if !p.expectKeyword("SET") {
		return nil, errors.New("expected SET keyword")
	}
	p.advance()

	// Parse SET assignments
	columns := []string{}
	values := []string{}

	for {
		// Get column name
		if !p.expectType(TokenIdentifier) {
			return nil, errors.New("expected column name")
		}
		columns = append(columns, p.current().Value)
		p.advance()

		// Verify equals sign
		if !p.expectType(TokenEquals) {
			return nil, errors.New("expected = after column name")
		}
		p.advance()

		// Get value
		if !p.expectType(TokenString) && !p.expectType(TokenNumber) {
			return nil, errors.New("expected value after =")
		}
		values = append(values, p.current().Value)
		p.advance()

		// Check if there are more assignments
		if p.currPos >= len(p.tokens) || p.current().Type != TokenComma {
			break
		}
		p.advance() // Skip comma
	}

	// Parse WHERE clause if present
	conditions := []Condition{}

	// Check if there's a WHERE clause and we haven't reached EOF
	if p.currPos < len(p.tokens) && p.current().Type == TokenKeyword && p.current().Value == "WHERE" {
		p.advance()

		// Check if we still have tokens
		if p.currPos >= len(p.tokens) {
			return nil, errors.New("unexpected end of input after WHERE")
		}

		// Get the left side of the condition
		if !p.expectType(TokenIdentifier) {
			return nil, fmt.Errorf("expected column name in WHERE clause, got %s", TokenToString(p.current()))
		}
		left := p.current().Value
		p.advance()

		// Check we still have tokens for the operator
		if p.currPos >= len(p.tokens) {
			return nil, errors.New("unexpected end of input, expected operator")
		}

		// Get the operator
		if p.current().Type != TokenEquals && p.current().Type != TokenOperator {
			return nil, fmt.Errorf("expected comparison operator in WHERE clause, got %s", TokenToString(p.current()))
		}

		var operator string
		if p.current().Type == TokenEquals {
			operator = "="
		} else {
			operator = p.current().Value
		}
		p.advance()

		// Check we still have tokens for the value
		if p.currPos >= len(p.tokens) {
			return nil, errors.New("unexpected end of input, expected value")
		}

		// Get the value
		var right string
		if p.current().Type == TokenString {
			right = p.current().Value
		} else if p.current().Type == TokenNumber {
			right = p.current().Value
		} else {
			return nil, fmt.Errorf("expected string or number value in WHERE clause, got %s", TokenToString(p.current()))
		}
		p.advance()

		// Add the condition
		conditions = append(conditions, Condition{
			Left:     left,
			Operator: operator,
			Right:    right,
		})
	}

	return UpdateNode{
		TableName:  tableName,
		Columns:    columns,
		Values:     values,
		Conditions: conditions,
	}, nil
}

// parseDropTable parses a DROP TABLE statement
func (p *Parser) parseDropTable() (Node, error) {
	// Verify "DROP"
	if !p.expectKeyword("DROP") {
		return nil, errors.New("expected DROP keyword")
	}
	p.advance()

	// Verify "TABLE"
	if !p.expectKeyword("TABLE") {
		return nil, errors.New("expected TABLE keyword")
	}
	p.advance()

	// Get table name
	if !p.expectType(TokenIdentifier) {
		return nil, errors.New("expected table name")
	}
	tableName := p.current().Value
	p.advance()

	return DropTableNode{
		TableName: tableName,
	}, nil
}

// Helper methods

func (p *Parser) current() Token {
	if p.currPos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.currPos]
}

func (p *Parser) advance() {
	p.currPos++
}

func (p *Parser) expectType(t TokenType) bool {
	if p.currPos >= len(p.tokens) {
		return false
	}
	return p.tokens[p.currPos].Type == t
}

func (p *Parser) expectKeyword(kw string) bool {
	return p.currPos < len(p.tokens) &&
		p.tokens[p.currPos].Type == TokenKeyword &&
		p.tokens[p.currPos].Value == kw
}
