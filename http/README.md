# 🌐 HTTP API 服务

Bitcask 的 HTTP API 接口，提供基于 RESTful 风格的键值存储服务。

## ✨ 功能特性

- 🔄 基于 RESTful 设计理念
- 📝 支持 JSON 格式的请求和响应
- 🗝️ 提供键值存储的基本操作
- 📊 支持各种数据类型（字符串、列表、哈希等）
- 📚 包含 Swagger 文档

## 📡 API 接口

### 🧪 基础操作

#### 💓 健康检查
- `GET /health` - 检查服务健康状态

#### ℹ️ 服务信息
- `GET /info` - 获取服务信息，包括版本、运行时间等

### 🔑 键值操作

#### 📝 字符串操作
- `GET /key/:key` - 获取指定键的值
- `PUT /key/:key` - 设置键值对
- `DELETE /key/:key` - 删除指定键
- `GET /keys` - 列出所有键或匹配模式的键

#### ⏱️ 过期时间
- `PUT /key/:key/expire` - 设置键的过期时间
- `GET /key/:key/ttl` - 获取键的剩余过期时间

### 🏗️ 数据结构操作

#### 📋 列表操作
- `POST /list/:key` - 在列表左侧/右侧添加元素
- `GET /list/:key` - 获取列表的元素
- `DELETE /list/:key` - 从列表删除元素

#### 📑 哈希表操作
- `POST /hash/:key` - 设置哈希表字段
- `GET /hash/:key` - 获取哈希表字段
- `DELETE /hash/:key/:field` - 删除哈希表字段

#### 🔢 集合操作
- `POST /set/:key` - 添加集合元素
- `GET /set/:key` - 获取集合元素
- `DELETE /set/:key/:member` - 删除集合成员

### ⚙️ 管理接口

#### 🔧 维护操作
- `POST /admin/hint` - 生成 hint 文件
- `POST /admin/merge` - 执行合并操作
- `GET /admin/stats` - 获取统计信息

## 🔄 请求/响应格式

### 📋 通用响应格式

```json
{
  "status": "success|error",
  "data": {...},
  "message": "错误信息（仅在错误时出现）"
}
```

### 📝 示例

#### 📥 设置键值对

请求:
```
PUT /key/mykey
Content-Type: application/json

{
  "value": "myvalue",
  "ttl": 3600
}
```

响应:
```json
{
  "status": "success",
  "data": {
    "key": "mykey"
  }
}
```

#### 📤 获取键值

请求:
```
GET /key/mykey
```

响应:
```json
{
  "status": "success",
  "data": {
    "key": "mykey",
    "value": "myvalue",
    "ttl": 3540
  }
}
```

## 🚀 使用方法

### 🏁 启动HTTP服务

```bash
bitcask http --addr :8080 --data-dir ./data
```

### ⚙️ 配置选项

- `--addr` - 服务监听地址，默认 `:8080`
- `--data-dir` - 数据目录路径
- `--cors` - 是否启用跨域资源共享
- `--swagger` - 是否启用Swagger文档

## 📚 API 文档

访问 `/swagger/index.html` 获取完整的 API 文档。

## 🔒 安全性考虑

- HTTP 服务默认不包含认证机制，建议在生产环境中添加认证层
- 可以通过反向代理（如 Nginx）添加 SSL/TLS 支持
- 敏感操作应限制访问来源 