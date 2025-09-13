@echo off

REM 确保file目录存在
if not exist "file" mkdir file

REM 启动Spring Boot应用，支持后台运行
echo 启动文件服务器...
start /B java -jar web.jar
echo 文件服务器已在后台启动
echo 访问地址: http://localhost:8080
echo 按任意键退出...
pause