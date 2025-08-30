@echo off

REM 确保file目录存在
if not exist "file" mkdir file

REM 启动Spring Boot应用
java -jar web.jar