package main

import (
	"fmt"
	"log"
	"time"

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

	// 检查temp-key是否存在
	value, err := redis.String(conn.Do("GET", "temp-key"))
	if err != nil {
		if err == redis.ErrNil {
			fmt.Println("temp-key 已经过期，无法获取")
		} else {
			log.Printf("获取temp-key错误: %v", err)
		}
	} else {
		fmt.Printf("temp-key仍然存在，值为: %s\n", value)

		// 等待5秒钟再次检查
		fmt.Println("等待5秒钟后再次检查...")
		time.Sleep(5 * time.Second)

		value, err = redis.String(conn.Do("GET", "temp-key"))
		if err != nil {
			if err == redis.ErrNil {
				fmt.Println("等待后，temp-key 已经过期，无法获取")
			} else {
				log.Printf("获取temp-key错误: %v", err)
			}
		} else {
			fmt.Printf("等待后，temp-key仍然存在，值为: %s\n", value)
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
		}
	}

	fmt.Println("\n测试完成")
}
