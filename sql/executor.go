package sql

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aixiasang/bitcask"
)

// Executor handles the execution of SQL statements
type Executor struct {
	db *bitcask.Bitcask
}

// NewExecutor creates a new executor with the given bitcask instance
func NewExecutor(db *bitcask.Bitcask) *Executor {
	return &Executor{db: db}
}

// TableSchema represents a table's schema
type TableSchema struct {
	Name    string      `json:"name"`
	Columns []ColumnDef `json:"columns"`
}

// Row represents a row of data
type Row map[string]string

// QueryResult represents the result of a query
type QueryResult struct {
	Columns []string `json:"columns"`
	Rows    []Row    `json:"rows"`
}

// Execute executes a SQL statement
func (e *Executor) Execute(node Node) (*QueryResult, error) {
	switch n := node.(type) {
	case CreateTableNode:
		return e.executeCreateTable(n)
	case InsertNode:
		return e.executeInsert(n)
	case SelectNode:
		return e.executeSelect(n)
	case DeleteNode:
		return e.executeDelete(n)
	case UpdateNode:
		return e.executeUpdate(n)
	case DropTableNode:
		return e.executeDropTable(n)
	default:
		return nil, fmt.Errorf("unsupported statement type: %s", n.Type())
	}
}

// executeCreateTable executes a CREATE TABLE statement
func (e *Executor) executeCreateTable(node CreateTableNode) (*QueryResult, error) {
	// Check if the table already exists
	tableKey := fmt.Sprintf("__schema_%s", node.TableName)
	_, exists := e.db.Get([]byte(tableKey))
	if exists {
		return nil, fmt.Errorf("table '%s' already exists", node.TableName)
	}

	// Create a new schema
	schema := TableSchema{
		Name:    node.TableName,
		Columns: node.Columns,
	}

	// Serialize the schema to JSON
	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize schema: %v", err)
	}

	// Store the schema in the database
	if err := e.db.Put([]byte(tableKey), schemaBytes); err != nil {
		return nil, fmt.Errorf("failed to store schema: %v", err)
	}

	return &QueryResult{}, nil
}

// executeInsert executes an INSERT statement
func (e *Executor) executeInsert(node InsertNode) (*QueryResult, error) {
	// Get the table schema
	tableKey := fmt.Sprintf("__schema_%s", node.TableName)
	schemaData, exists := e.db.Get([]byte(tableKey))
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", node.TableName)
	}

	// Deserialize the schema
	var schema TableSchema
	if err := json.Unmarshal(schemaData, &schema); err != nil {
		return nil, fmt.Errorf("failed to deserialize schema: %v", err)
	}

	// Validate column names
	for _, col := range node.Columns {
		found := false
		for _, schemaCol := range schema.Columns {
			if strings.EqualFold(col, schemaCol.Name) {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column '%s' does not exist in table '%s'", col, node.TableName)
		}
	}

	// Find primary key column
	var pkColumn string
	for _, col := range schema.Columns {
		if col.PrimaryKey {
			pkColumn = col.Name
			break
		}
	}

	// If no primary key column is defined, use the first column
	if pkColumn == "" && len(schema.Columns) > 0 {
		pkColumn = schema.Columns[0].Name
	}

	// Insert each row
	for _, rowValues := range node.Values {
		if len(rowValues) != len(node.Columns) {
			return nil, fmt.Errorf("number of values does not match number of columns")
		}

		// Create a row object
		row := make(Row)
		for i, col := range node.Columns {
			row[col] = rowValues[i]
		}

		// Find the primary key value
		pkValue := ""
		for i, col := range node.Columns {
			if strings.EqualFold(col, pkColumn) {
				pkValue = rowValues[i]
				break
			}
		}

		if pkValue == "" {
			return nil, errors.New("primary key value is required")
		}

		// Create a key for this row
		rowKey := fmt.Sprintf("%s:%s", node.TableName, pkValue)

		// Serialize the row to JSON
		rowBytes, err := json.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize row: %v", err)
		}

		// Store the row in the database
		if err := e.db.Put([]byte(rowKey), rowBytes); err != nil {
			return nil, fmt.Errorf("failed to store row: %v", err)
		}
	}

	return &QueryResult{}, nil
}

// executeSelect executes a SELECT statement
func (e *Executor) executeSelect(node SelectNode) (*QueryResult, error) {
	// Get the table schema
	tableKey := fmt.Sprintf("__schema_%s", node.TableName)
	schemaData, exists := e.db.Get([]byte(tableKey))
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", node.TableName)
	}

	// Deserialize the schema
	var schema TableSchema
	if err := json.Unmarshal(schemaData, &schema); err != nil {
		return nil, fmt.Errorf("failed to deserialize schema: %v", err)
	}

	// Determine which columns to include in the result
	var columns []string
	if len(node.Columns) == 0 || (len(node.Columns) == 1 && node.Columns[0] == "*") {
		// Include all columns in the schema
		for _, col := range schema.Columns {
			columns = append(columns, col.Name)
		}
	} else {
		// Include only the specified columns
		columns = node.Columns

		// Validate that all specified columns exist in the schema
		for _, col := range columns {
			found := false
			for _, schemaCol := range schema.Columns {
				if strings.EqualFold(col, schemaCol.Name) {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column '%s' does not exist in table '%s'", col, node.TableName)
			}
		}
	}

	// Try an optimized lookup if it's a primary key condition
	if canUseDirectLookup(node, schema) {
		_, pkValue := getDirectLookupKey(node, schema)
		rowKey := fmt.Sprintf("%s:%s", node.TableName, pkValue)

		// Try to get the row directly
		rowData, exists := e.db.Get([]byte(rowKey))
		if exists {
			var row Row
			if err := json.Unmarshal(rowData, &row); err != nil {
				return nil, fmt.Errorf("failed to deserialize row: %v", err)
			}

			// Check if the row matches the WHERE conditions
			if matchesAllConditions(row, node.Conditions) {
				// Create a result row with only the requested columns
				resultRow := make(Row)
				for _, col := range columns {
					resultRow[col] = row[col]
				}

				return &QueryResult{
					Columns: columns,
					Rows:    []Row{resultRow},
				}, nil
			}
		}

		// If we got here, no rows matched or the row didn't match all conditions
		return &QueryResult{
			Columns: columns,
			Rows:    []Row{},
		}, nil
	}

	// Otherwise, we need to scan all rows
	// Use Scan with prefix check instead of ScanRange
	var result QueryResult
	result.Columns = columns

	prefix := fmt.Sprintf("%s:", node.TableName)

	// First collect all potential rows
	var rowsToCheck []Row

	err := e.db.Scan(func(key []byte, value []byte) error {
		keyStr := string(key)
		if strings.HasPrefix(keyStr, prefix) {
			var row Row
			if err := json.Unmarshal(value, &row); err != nil {
				return fmt.Errorf("failed to deserialize row: %v", err)
			}

			rowsToCheck = append(rowsToCheck, row)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan for rows: %v", err)
	}

	// Now filter the rows based on the WHERE conditions
	for _, row := range rowsToCheck {
		if matchesAllConditions(row, node.Conditions) {
			// Create a result row with only the requested columns
			resultRow := make(Row)
			for _, col := range columns {
				resultRow[col] = row[col]
			}

			result.Rows = append(result.Rows, resultRow)
		}
	}

	return &result, nil
}

// Helper function to check if a direct lookup can be used
func canUseDirectLookup(node SelectNode, schema TableSchema) bool {
	// We need to have WHERE conditions and know the primary key
	if len(node.Conditions) == 0 {
		return false
	}

	// Find the primary key column
	var pkColumn string
	for _, col := range schema.Columns {
		if col.PrimaryKey {
			pkColumn = col.Name
			break
		}
	}

	// If no primary key, use the first column as default
	if pkColumn == "" && len(schema.Columns) > 0 {
		pkColumn = schema.Columns[0].Name
	}

	// Check if one of the conditions is for the primary key with equality
	for _, cond := range node.Conditions {
		if strings.EqualFold(cond.Left, pkColumn) && cond.Operator == "=" {
			return true
		}
	}

	return false
}

// Helper function to get the primary key value for direct lookup
func getDirectLookupKey(node SelectNode, schema TableSchema) (string, string) {
	// Find the primary key column
	var pkColumn string
	for _, col := range schema.Columns {
		if col.PrimaryKey {
			pkColumn = col.Name
			break
		}
	}

	// If no primary key, use the first column as default
	if pkColumn == "" && len(schema.Columns) > 0 {
		pkColumn = schema.Columns[0].Name
	}

	// Find the condition with the primary key
	for _, cond := range node.Conditions {
		if strings.EqualFold(cond.Left, pkColumn) && cond.Operator == "=" {
			return pkColumn, cond.Right
		}
	}

	return "", ""
}

// Helper function to check if a row matches all WHERE conditions
func matchesAllConditions(row Row, conditions []Condition) bool {
	for _, cond := range conditions {
		value, exists := row[cond.Left]
		if !exists {
			return false
		}

		switch cond.Operator {
		case "=":
			if value != cond.Right {
				return false
			}
		case ">":
			// Try numeric comparison first
			leftNum, leftErr := strconv.ParseFloat(value, 64)
			rightNum, rightErr := strconv.ParseFloat(cond.Right, 64)

			if leftErr == nil && rightErr == nil {
				// Both are valid numbers
				if leftNum <= rightNum {
					return false
				}
			} else {
				// String comparison
				if value <= cond.Right {
					return false
				}
			}
		case "<":
			// Try numeric comparison first
			leftNum, leftErr := strconv.ParseFloat(value, 64)
			rightNum, rightErr := strconv.ParseFloat(cond.Right, 64)

			if leftErr == nil && rightErr == nil {
				// Both are valid numbers
				if leftNum >= rightNum {
					return false
				}
			} else {
				// String comparison
				if value >= cond.Right {
					return false
				}
			}
		case ">=":
			// Try numeric comparison first
			leftNum, leftErr := strconv.ParseFloat(value, 64)
			rightNum, rightErr := strconv.ParseFloat(cond.Right, 64)

			if leftErr == nil && rightErr == nil {
				// Both are valid numbers
				if leftNum < rightNum {
					return false
				}
			} else {
				// String comparison
				if value < cond.Right {
					return false
				}
			}
		case "<=":
			// Try numeric comparison first
			leftNum, leftErr := strconv.ParseFloat(value, 64)
			rightNum, rightErr := strconv.ParseFloat(cond.Right, 64)

			if leftErr == nil && rightErr == nil {
				// Both are valid numbers
				if leftNum > rightNum {
					return false
				}
			} else {
				// String comparison
				if value > cond.Right {
					return false
				}
			}
		default:
			// Unsupported operator
			return false
		}
	}
	return true
}

// executeDelete executes a DELETE statement
func (e *Executor) executeDelete(node DeleteNode) (*QueryResult, error) {
	// Get the table schema
	tableKey := fmt.Sprintf("__schema_%s", node.TableName)
	schemaData, exists := e.db.Get([]byte(tableKey))
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", node.TableName)
	}

	// Deserialize the schema
	var schema TableSchema
	if err := json.Unmarshal(schemaData, &schema); err != nil {
		return nil, fmt.Errorf("failed to deserialize schema: %v", err)
	}

	// If there are no conditions, delete all rows
	if len(node.Conditions) == 0 {
		// Use Scan with prefix check instead of ScanRange
		deletedCount := 0
		prefix := fmt.Sprintf("%s:", node.TableName)

		// Scan all keys and collect those that match our prefix
		var keysToDelete [][]byte
		err := e.db.Scan(func(key []byte, value []byte) error {
			keyStr := string(key)
			if strings.HasPrefix(keyStr, prefix) {
				keysToDelete = append(keysToDelete, key)
			}
			return nil
		})

		if err != nil {
			return nil, fmt.Errorf("failed to scan for rows: %v", err)
		}

		// Delete all the matched keys
		for _, key := range keysToDelete {
			if err := e.db.Delete(key); err != nil {
				return nil, fmt.Errorf("failed to delete row: %v", err)
			}
			deletedCount++
		}

		return &QueryResult{
			Columns: []string{"deleted_count"},
			Rows: []Row{
				{"deleted_count": strconv.Itoa(deletedCount)},
			},
		}, nil
	}

	// If there are WHERE conditions, we need to find the matching rows

	// Try an optimized lookup if it's a primary key condition
	if canUseDirectLookup(SelectNode{TableName: node.TableName, Conditions: node.Conditions}, schema) {
		_, pkValue := getDirectLookupKey(SelectNode{TableName: node.TableName, Conditions: node.Conditions}, schema)
		rowKey := fmt.Sprintf("%s:%s", node.TableName, pkValue)

		// Try to get the row directly
		rowData, exists := e.db.Get([]byte(rowKey))
		if exists {
			var row Row
			if err := json.Unmarshal(rowData, &row); err != nil {
				return nil, fmt.Errorf("failed to deserialize row: %v", err)
			}

			// Check if the row matches the WHERE conditions
			if matchesAllConditions(row, node.Conditions) {
				// Delete the row
				if err := e.db.Delete([]byte(rowKey)); err != nil {
					return nil, fmt.Errorf("failed to delete row: %v", err)
				}

				return &QueryResult{
					Columns: []string{"deleted_count"},
					Rows: []Row{
						{"deleted_count": "1"},
					},
				}, nil
			}
		}

		return &QueryResult{
			Columns: []string{"deleted_count"},
			Rows: []Row{
				{"deleted_count": "0"},
			},
		}, nil
	}

	// Otherwise, scan all keys and check conditions
	deletedCount := 0
	prefix := fmt.Sprintf("%s:", node.TableName)

	// First collect all potential rows
	var rowsToCheck []struct {
		key []byte
		row Row
	}

	err := e.db.Scan(func(key []byte, value []byte) error {
		keyStr := string(key)
		if strings.HasPrefix(keyStr, prefix) {
			var row Row
			if err := json.Unmarshal(value, &row); err != nil {
				return fmt.Errorf("failed to deserialize row: %v", err)
			}

			rowsToCheck = append(rowsToCheck, struct {
				key []byte
				row Row
			}{key, row})
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan for rows: %v", err)
	}

	// Now check each row against the conditions
	for _, item := range rowsToCheck {
		if matchesAllConditions(item.row, node.Conditions) {
			if err := e.db.Delete(item.key); err != nil {
				return nil, fmt.Errorf("failed to delete row: %v", err)
			}
			deletedCount++
		}
	}

	return &QueryResult{
		Columns: []string{"deleted_count"},
		Rows: []Row{
			{"deleted_count": strconv.Itoa(deletedCount)},
		},
	}, nil
}

// executeUpdate executes an UPDATE statement
func (e *Executor) executeUpdate(node UpdateNode) (*QueryResult, error) {
	// Get the table schema
	tableKey := fmt.Sprintf("__schema_%s", node.TableName)
	schemaData, exists := e.db.Get([]byte(tableKey))
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", node.TableName)
	}

	// Deserialize the schema
	var schema TableSchema
	if err := json.Unmarshal(schemaData, &schema); err != nil {
		return nil, fmt.Errorf("failed to deserialize schema: %v", err)
	}

	// Validate the columns
	for _, col := range node.Columns {
		found := false
		for _, schemaCol := range schema.Columns {
			if strings.EqualFold(col, schemaCol.Name) {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column '%s' does not exist in table '%s'", col, node.TableName)
		}
	}

	// Find primary key column
	var pkColumn string
	for _, col := range schema.Columns {
		if col.PrimaryKey {
			pkColumn = col.Name
			break
		}
	}

	// If no primary key column is defined, use the first column
	if pkColumn == "" && len(schema.Columns) > 0 {
		pkColumn = schema.Columns[0].Name
	}

	// If there are WHERE conditions for a specific primary key, try optimized lookup
	if canUseDirectLookup(SelectNode{TableName: node.TableName, Conditions: node.Conditions}, schema) {
		_, pkValue := getDirectLookupKey(SelectNode{TableName: node.TableName, Conditions: node.Conditions}, schema)
		rowKey := fmt.Sprintf("%s:%s", node.TableName, pkValue)

		// Try to get the row directly
		rowData, exists := e.db.Get([]byte(rowKey))
		if exists {
			var row Row
			if err := json.Unmarshal(rowData, &row); err != nil {
				return nil, fmt.Errorf("failed to deserialize row: %v", err)
			}

			// Check if the row matches the WHERE conditions
			if matchesAllConditions(row, node.Conditions) {
				// Update the row with the new values
				for i, col := range node.Columns {
					row[col] = node.Values[i]
				}

				// Serialize the row to JSON
				rowBytes, err := json.Marshal(row)
				if err != nil {
					return nil, fmt.Errorf("failed to serialize row: %v", err)
				}

				// Store the updated row in the database
				if err := e.db.Put([]byte(rowKey), rowBytes); err != nil {
					return nil, fmt.Errorf("failed to store row: %v", err)
				}

				return &QueryResult{
					Columns: []string{"updated_count"},
					Rows: []Row{
						{"updated_count": "1"},
					},
				}, nil
			}
		}

		return &QueryResult{
			Columns: []string{"updated_count"},
			Rows: []Row{
				{"updated_count": "0"},
			},
		}, nil
	}

	// Otherwise, perform a full table scan
	// Determine the start and end keys for the scan
	startKey := fmt.Sprintf("%s:", node.TableName)
	endKey := fmt.Sprintf("%s;", node.TableName) // Using ; as it's the next ASCII character after :

	// Scan the table
	rowResults, err := e.db.ScanRange([]byte(startKey), []byte(endKey))
	if err != nil && err != bitcask.ErrExceedEndRange {
		return nil, fmt.Errorf("failed to scan table: %v", err)
	}

	// Process each row
	updatedCount := 0
	for _, rowResult := range rowResults {
		var row Row
		if err := json.Unmarshal(rowResult.Value, &row); err != nil {
			return nil, fmt.Errorf("failed to deserialize row: %v", err)
		}

		// Check if the row matches the WHERE conditions
		if matchesAllConditions(row, node.Conditions) {
			// Update the row with the new values
			for i, col := range node.Columns {
				row[col] = node.Values[i]
			}

			// Serialize the row to JSON
			rowBytes, err := json.Marshal(row)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize row: %v", err)
			}

			// Store the updated row in the database
			if err := e.db.Put(rowResult.Key, rowBytes); err != nil {
				return nil, fmt.Errorf("failed to store row: %v", err)
			}
			updatedCount++
		}
	}

	return &QueryResult{
		Columns: []string{"updated_count"},
		Rows: []Row{
			{"updated_count": strconv.Itoa(updatedCount)},
		},
	}, nil
}

// executeDropTable executes a DROP TABLE statement
func (e *Executor) executeDropTable(node DropTableNode) (*QueryResult, error) {
	// Get the table schema
	tableKey := fmt.Sprintf("__schema_%s", node.TableName)
	_, exists := e.db.Get([]byte(tableKey))
	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", node.TableName)
	}

	// Delete the schema
	if err := e.db.Delete([]byte(tableKey)); err != nil {
		return nil, fmt.Errorf("failed to delete table schema: %v", err)
	}

	// Instead of using ScanRange, use Scan with a prefix check to find and delete rows
	deletedCount := 0
	prefix := fmt.Sprintf("%s:", node.TableName)

	// Scan all keys and collect those that match our prefix
	var keysToDelete [][]byte
	err := e.db.Scan(func(key []byte, value []byte) error {
		keyStr := string(key)
		if strings.HasPrefix(keyStr, prefix) {
			keysToDelete = append(keysToDelete, key)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan for table rows: %v", err)
	}

	// Now delete all the matched keys
	for _, key := range keysToDelete {
		if err := e.db.Delete(key); err != nil {
			return nil, fmt.Errorf("failed to delete row: %v", err)
		}
		deletedCount++
	}

	return &QueryResult{
		Columns: []string{"dropped_table", "deleted_rows"},
		Rows: []Row{
			{"dropped_table": node.TableName, "deleted_rows": strconv.Itoa(deletedCount)},
		},
	}, nil
}
