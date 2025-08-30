# 文件上传下载应用

这是一个基于Spring Boot的文件上传下载应用，可以在JDK 8及以上版本运行。

## 功能特性

- ✅ 无限制的文件大小上传
- ✅ 文件存储到相对路径 `file` 目录
- ✅ 提供文件下载功能
- ✅ 支持通过Linux命令行上传文件

## 快速开始

### 1. 编译打包

```bash
mvn clean package
```

打包后会在 `target` 目录生成 `webapp.zip` 文件，包含:
- web.jar (应用程序jar包)
- file 目录 (文件存储目录)
- start.sh (Linux/Mac启动脚本)
- start.bat (Windows启动脚本)

### 2. 运行应用

**Linux/Mac:**

```bash
sh start.sh
```

**Windows:**

```cmd
start.bat
```

应用会在 http://localhost:8080 启动

## 通过Linux命令行上传文件

可以使用curl命令从Linux命令行上传文件到应用程序。例如，要上传 `/home/snail/aaa.zip` 文件：

```bash
curl -F "file=@/home/snail/aaa.zip" http://localhost:8080/api/files/upload
```

上传成功后，会返回类似以下的响应：

```
文件上传成功: 20231220153045-123e4567-e89b-12d3-a456-426614174000.zip
```

## 下载文件

使用浏览器或curl命令下载文件：

```bash
curl -OJ "http://localhost:8080/download?name=文件名"
```

## 注意事项

1. 上传的文件会保存在应用程序同级的 `file` 目录中
2. 文件名会自动生成唯一标识，避免覆盖
3. 应用支持上传任何类型的文件，没有大小限制