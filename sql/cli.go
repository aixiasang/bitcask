package sql

import (
	"fmt"
	"strings"

	"github.com/aixiasang/bitcask"
	"github.com/spf13/cobra"
)

// RegisterCommand registers the SQL command with the root command
func RegisterCommand(rootCmd *cobra.Command, bcCreator func() (*bitcask.Bitcask, error)) {
	var sqlCmd = &cobra.Command{
		Use:   "sql [SQL statement]",
		Short: "Execute SQL statements on the bitcask database",
		Long: `Execute SQL statements on the bitcask database.
Supported statements:
  - CREATE TABLE tablename (column1 type1, column2 type2 PRIMARY KEY, ...)
  - INSERT INTO tablename (column1, column2, ...) VALUES (value1, value2, ...), ...
  - SELECT column1, column2, ... FROM tablename [WHERE condition]
  - SELECT * FROM tablename [WHERE condition]`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			// Create the bitcask instance
			bc, err := bcCreator()
			if err != nil {
				fmt.Printf("创建 Bitcask 实例失败: %v\n", err)
				return
			}
			defer bc.Close()

			// Join all arguments into a single SQL statement
			sqlStatement := strings.Join(args, " ")

			// Parse the SQL statement
			node, err := Parse(sqlStatement)
			if err != nil {
				fmt.Printf("SQL 解析错误: %v\n", err)
				return
			}

			// Execute the SQL statement
			executor := NewExecutor(bc)
			result, err := executor.Execute(node)
			if err != nil {
				fmt.Printf("SQL 执行错误: %v\n", err)
				return
			}

			// If this is a query with results, print them
			if len(result.Columns) > 0 && len(result.Rows) > 0 {
				// Print column headers
				fmt.Print("| ")
				for _, col := range result.Columns {
					fmt.Printf("%s\t", col)
				}
				fmt.Println()

				// Print separator
				fmt.Print("+-")
				for _, col := range result.Columns {
					for i := 0; i < len(col); i++ {
						fmt.Print("-")
					}
					fmt.Print("--\t")
				}
				fmt.Println()

				// Print rows
				for _, row := range result.Rows {
					fmt.Print("| ")
					for _, col := range result.Columns {
						fmt.Printf("%s\t", row[col])
					}
					fmt.Println()
				}
				fmt.Printf("结果集: %d 行\n", len(result.Rows))
			} else {
				fmt.Println("执行成功")
			}
		},
	}

	// Add the SQL shell command
	var sqlShellCmd = &cobra.Command{
		Use:   "sqlshell",
		Short: "Start an interactive SQL shell",
		Long: `Start an interactive SQL shell to execute SQL statements.
Enter SQL statements at the prompt and press Enter to execute.
Type 'exit' or 'quit' to exit the shell.`,
		Run: func(cmd *cobra.Command, args []string) {
			// Create the bitcask instance
			bc, err := bcCreator()
			if err != nil {
				fmt.Printf("创建 Bitcask 实例失败: %v\n", err)
				return
			}
			defer bc.Close()

			executor := NewExecutor(bc)

			fmt.Println("SQL 交互式模式已启动。输入 SQL 语句并按 Enter 执行。")
			fmt.Println("输入 'exit' 或 'quit' 退出。")

			scanner := NewSQLScanner()
			for {
				fmt.Print("sql> ")

				sqlStatement, err := scanner.ReadStatement()
				if err != nil {
					fmt.Printf("读取输入错误: %v\n", err)
					continue
				}

				// Check for exit command
				sqlStatement = strings.TrimSpace(sqlStatement)
				if sqlStatement == "" {
					continue
				}
				if sqlStatement == "exit" || sqlStatement == "quit" {
					fmt.Println("再见!")
					break
				}

				// Parse and execute the SQL statement
				node, err := Parse(sqlStatement)
				if err != nil {
					fmt.Printf("SQL 解析错误: %v\n", err)
					continue
				}

				result, err := executor.Execute(node)
				if err != nil {
					fmt.Printf("SQL 执行错误: %v\n", err)
					continue
				}

				// If this is a query with results, print them
				if len(result.Columns) > 0 && len(result.Rows) > 0 {
					// Print column headers
					fmt.Print("| ")
					for _, col := range result.Columns {
						fmt.Printf("%s\t", col)
					}
					fmt.Println()

					// Print separator
					fmt.Print("+-")
					for _, col := range result.Columns {
						for i := 0; i < len(col); i++ {
							fmt.Print("-")
						}
						fmt.Print("--\t")
					}
					fmt.Println()

					// Print rows
					for _, row := range result.Rows {
						fmt.Print("| ")
						for _, col := range result.Columns {
							fmt.Printf("%s\t", row[col])
						}
						fmt.Println()
					}
					fmt.Printf("结果集: %d 行\n", len(result.Rows))
				} else {
					fmt.Println("执行成功")
				}
			}
		},
	}

	// Add the SQL commands to the root command
	rootCmd.AddCommand(sqlCmd)
	rootCmd.AddCommand(sqlShellCmd)
}

// SQLScanner reads SQL statements from standard input
type SQLScanner struct {
	buffer string
}

// NewSQLScanner creates a new SQL scanner
func NewSQLScanner() *SQLScanner {
	return &SQLScanner{
		buffer: "",
	}
}

// ReadStatement reads a complete SQL statement
func (s *SQLScanner) ReadStatement() (string, error) {
	var input string
	var err error

	// Read lines until we have a complete statement
	for {
		var line string
		fmt.Scanln(&line)

		// Check for errors
		if err != nil {
			return "", err
		}

		// Check for exit command
		if strings.TrimSpace(line) == "exit" || strings.TrimSpace(line) == "quit" {
			return strings.TrimSpace(line), nil
		}

		// Append the line to the buffer
		if s.buffer == "" {
			s.buffer = line
		} else {
			s.buffer += " " + line
		}

		// Check if we have a complete statement
		if strings.HasSuffix(strings.TrimSpace(s.buffer), ";") {
			statement := strings.TrimSpace(s.buffer)
			s.buffer = ""
			return statement[:len(statement)-1], nil // Remove the trailing semicolon
		}

		// Special case: if the line is a complete statement without a semicolon, return it
		if input == "" && !strings.Contains(line, ";") {
			statement := strings.TrimSpace(s.buffer)
			s.buffer = ""
			return statement, nil
		}
	}
}
