#!/bin/bash

# 确保file目录存在
if [ ! -d "file" ]; then
  mkdir file
fi

# 启动Spring Boot应用
java -jar web.jar