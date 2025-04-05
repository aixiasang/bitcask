package main

import (
	"fmt"
	"log"
	"os"

	"github.com/gomodule/redigo/redis"
)

func main() {
	// 连接到Redis服务器
	conn, err := redis.Dial("tcp", "localhost:6380")
	if err != nil {
		log.Fatalf("无法连接到Redis服务器: %v", err)
	}
	defer conn.Close()

	fmt.Println("已成功连接到Redis服务器")

	// 测试PING
	pong, err := redis.String(conn.Do("PING"))
	if err != nil {
		log.Printf("PING错误: %v", err)
	} else {
		fmt.Printf("PING结果: %s\n", pong)
	}

	// 测试INFO
	info, err := redis.String(conn.Do("INFO"))
	if err != nil {
		log.Printf("INFO错误: %v", err)
	} else {
		fmt.Printf("INFO返回的服务器信息:\n%s\n", info)
	}

	// 测试基本的字符串操作
	fmt.Println("\n=== 测试字符串操作 ===")
	_, err = conn.Do("SET", "test-key", "test-value")
	if err != nil {
		log.Printf("SET错误: %v", err)
	} else {
		value, err := redis.String(conn.Do("GET", "test-key"))
		if err != nil {
			log.Printf("GET错误: %v", err)
		} else {
			fmt.Printf("GET %s = %s\n", "test-key", value)
		}
	}

	// 检查testdata中生成的键
	fmt.Println("\n=== 测试从testdata访问数据 ===")
	// 检查字符串键
	keys := []string{
		"string:key1", "string:key2", "string:key3", "string:key4",
	}

	for _, key := range keys {
		value, err := redis.String(conn.Do("GET", key))
		if err != nil {
			log.Printf("获取键 %s 失败: %v", key, err)
		} else {
			fmt.Printf("GET %s = %s\n", key, value)
		}
	}

	// 列出所有键
	fmt.Println("\n=== 列出所有键 ===")
	allKeys, err := redis.Strings(conn.Do("KEYS", "*"))
	if err != nil {
		log.Printf("KEYS错误: %v", err)
	} else {
		fmt.Printf("找到%d个键:\n", len(allKeys))
		for i, key := range allKeys {
			fmt.Printf("%d: %s\n", i+1, key)
			if i >= 9 { // 只显示前10个
				fmt.Println("... 以及更多")
				break
			}
		}
	}

	// 测试列表操作
	fmt.Println("\n=== 测试列表操作 ===")
	_, err = conn.Do("LPUSH", "mylist", "item1", "item2", "item3")
	if err != nil {
		log.Printf("LPUSH错误: %v", err)
	} else {
		items, err := redis.Strings(conn.Do("LRANGE", "mylist", 0, -1))
		if err != nil {
			log.Printf("LRANGE错误: %v", err)
		} else {
			fmt.Printf("列表包含 %d 个元素: %v\n", len(items), items)
		}

		// 测试LPOP
		item, err := redis.String(conn.Do("LPOP", "mylist"))
		if err != nil {
			log.Printf("LPOP错误: %v", err)
		} else {
			fmt.Printf("从列表头部弹出: %s\n", item)
		}
	}

	// 测试哈希表操作
	fmt.Println("\n=== 测试哈希表操作 ===")
	_, err = conn.Do("HSET", "user:1", "name", "张三", "age", "30", "email", "zhangsan@example.com")
	if err != nil {
		log.Printf("HSET错误: %v", err)
	} else {
		// 获取单个字段
		name, err := redis.String(conn.Do("HGET", "user:1", "name"))
		if err != nil {
			log.Printf("HGET错误: %v", err)
		} else {
			fmt.Printf("用户名: %s\n", name)
		}

		// 获取所有字段
		fields, err := redis.StringMap(conn.Do("HGETALL", "user:1"))
		if err != nil {
			log.Printf("HGETALL错误: %v", err)
		} else {
			fmt.Println("用户信息:")
			for k, v := range fields {
				fmt.Printf("  %s: %s\n", k, v)
			}
		}
	}

	// 测试集合操作
	fmt.Println("\n=== 测试集合操作 ===")
	_, err = conn.Do("SADD", "languages", "Go", "Python", "Java", "C++")
	if err != nil {
		log.Printf("SADD错误: %v", err)
	} else {
		// 检查成员是否存在
		isMember, err := redis.Int(conn.Do("SISMEMBER", "languages", "Go"))
		if err != nil {
			log.Printf("SISMEMBER错误: %v", err)
		} else {
			fmt.Printf("Go 是集合成员: %v\n", isMember == 1)
		}

		// 获取所有成员
		members, err := redis.Strings(conn.Do("SMEMBERS", "languages"))
		if err != nil {
			log.Printf("SMEMBERS错误: %v", err)
		} else {
			fmt.Printf("集合包含 %d 个元素: %v\n", len(members), members)
		}
	}

	// 测试过期时间
	fmt.Println("\n=== 测试过期时间 ===")
	_, err = conn.Do("SET", "temp-key", "这个键将在5秒后过期")
	if err != nil {
		log.Printf("SET错误: %v", err)
	} else {
		_, err = conn.Do("EXPIRE", "temp-key", 5)
		if err != nil {
			log.Printf("EXPIRE错误: %v", err)
		} else {
			// 获取剩余时间
			ttl, err := redis.Int(conn.Do("TTL", "temp-key"))
			if err != nil {
				log.Printf("TTL错误: %v", err)
			} else {
				fmt.Printf("temp-key 将在 %d 秒后过期\n", ttl)
			}
		}
	}

	fmt.Println("\n测试完成")
	os.Exit(0)
}
