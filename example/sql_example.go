package main

import (
	"fmt"

	"github.com/aixiasang/bitcask"
	"github.com/aixiasang/bitcask/config"
	"github.com/aixiasang/bitcask/sql"
)

func main() {
	// Create a temporary directory for our database
	// tempDir, err := os.MkdirTemp("", "sql_example")
	// if err != nil {
	// 	fmt.Printf("Failed to create temp directory: %v\n", err)
	// 	return
	// }
	// defer os.RemoveAll(tempDir) // Clean up when done

	// Configure and initialize Bitcask
	conf := config.NewConfig()
	// conf.DataDir = tempDir
	conf.MaxFileSize = 1024 * 1024 // 1MB
	conf.BTreeOrder = 32
	conf.AutoSync = false // For better performance in this example
	conf.Debug = false

	// Create a new bitcask instance
	bc, err := bitcask.NewBitcask(conf)
	if err != nil {
		fmt.Printf("Failed to create Bitcask: %v\n", err)
		return
	}
	defer bc.Close()

	// Create an SQL executor
	executor := sql.NewExecutor(bc)

	// Example 1: Create a table
	fmt.Println("Creating employees table...")
	createTable := "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, position TEXT, salary INTEGER)"
	node, err := sql.Parse(createTable)
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}
	_, err = executor.Execute(node)
	if err != nil {
		fmt.Printf("Execute error: %v\n", err)
		return
	}

	// Example 2: Insert data
	fmt.Println("Inserting employees...")
	employees := []string{
		"INSERT INTO employees (id, name, position, salary) VALUES (1, 'John Doe', 'Engineer', 85000)",
		"INSERT INTO employees (id, name, position, salary) VALUES (2, 'Jane Smith', 'Manager', 110000)",
		"INSERT INTO employees (id, name, position, salary) VALUES (3, 'Alice Brown', 'Designer', 75000)",
		"INSERT INTO employees (id, name, position, salary) VALUES (4, 'Bob Johnson', 'Developer', 90000)",
	}

	for _, insertSQL := range employees {
		node, err := sql.Parse(insertSQL)
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			return
		}
		_, err = executor.Execute(node)
		if err != nil {
			fmt.Printf("Execute error: %v\n", err)
			return
		}
	}

	// Example 3: Select all
	fmt.Println("\nSelecting all employees:")
	selectAll := "SELECT * FROM employees"
	node, err = sql.Parse(selectAll)
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}
	result, err := executor.Execute(node)
	if err != nil {
		fmt.Printf("Execute error: %v\n", err)
		return
	}
	printResult(result)

	// Example 4: Select with WHERE
	fmt.Println("\nSelecting employees with salary > 80000:")
	selectWhere := "SELECT id, name, salary FROM employees WHERE salary > 80000"
	node, err = sql.Parse(selectWhere)
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}
	result, err = executor.Execute(node)
	if err != nil {
		fmt.Printf("Execute error: %v\n", err)
		return
	}
	printResult(result)

	// Example 5: Update
	fmt.Println("\nUpdating employee ID 3:")
	updateSQL := "UPDATE employees SET position = 'Senior Designer', salary = 85000 WHERE id = 3"
	node, err = sql.Parse(updateSQL)
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}
	_, err = executor.Execute(node)
	if err != nil {
		fmt.Printf("Execute error: %v\n", err)
		return
	}

	// Verify update
	fmt.Println("Checking updated employee:")
	selectUpdated := "SELECT * FROM employees WHERE id = 3"
	node, err = sql.Parse(selectUpdated)
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}
	result, err = executor.Execute(node)
	if err != nil {
		fmt.Printf("Execute error: %v\n", err)
		return
	}
	printResult(result)

	// Example 6: Delete
	fmt.Println("\nDeleting employee ID 2:")
	deleteSQL := "DELETE FROM employees WHERE id = 2"
	node, err = sql.Parse(deleteSQL)
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}
	result, err = executor.Execute(node)
	if err != nil {
		fmt.Printf("Execute error: %v\n", err)
		return
	}
	fmt.Printf("Deleted %s records\n", result.Rows[0]["deleted_count"])

	// Verify remaining records
	fmt.Println("\nRemaining employees:")
	node, err = sql.Parse(selectAll)
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}
	result, err = executor.Execute(node)
	if err != nil {
		fmt.Printf("Execute error: %v\n", err)
		return
	}
	printResult(result)
}

// Helper function to print query results
func printResult(result *sql.QueryResult) {
	if result == nil || len(result.Rows) == 0 {
		fmt.Println("No results found")
		return
	}

	// Print column headers
	for i, col := range result.Columns {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(col)
	}
	fmt.Println()

	// Print separator
	for i := 0; i < len(result.Columns); i++ {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print("--------")
	}
	fmt.Println()

	// Print rows
	for _, row := range result.Rows {
		for i, col := range result.Columns {
			if i > 0 {
				fmt.Print("\t")
			}
			fmt.Print(row[col])
		}
		fmt.Println()
	}
	fmt.Printf("Total: %d rows\n", len(result.Rows))
}
