package com.example.fileupload.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@RestController
public class FileUploadController {

    @Value("${file.upload-dir}")
    private String uploadDir;

    @PostMapping("/api/files/upload")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file) {
        long requestStartTime = System.currentTimeMillis();

        if (file.isEmpty()) {
            return new ResponseEntity<>("请选择要上传的文件", HttpStatus.BAD_REQUEST);
        }

        try {
            long fileSize = file.getSize();
            String originalFilename = file.getOriginalFilename();

            System.out.println("========== 上传性能诊断 ==========");
            System.out.println("文件名: " + originalFilename);
            System.out.println("文件大小: " + formatFileSize(fileSize));
            System.out.println("请求到达时间: " + LocalDateTime.now());

            // 确保上传目录存在
            long dirCheckStart = System.currentTimeMillis();
            Path uploadPath = Paths.get(uploadDir);
            if (!Files.exists(uploadPath)) {
                Files.createDirectories(uploadPath);
            }
            long dirCheckEnd = System.currentTimeMillis();
            System.out.println("[1] 目录检查耗时: " + (dirCheckEnd - dirCheckStart) + "ms");

            // 生成唯一文件名
            long nameGenStart = System.currentTimeMillis();
            String fileExtension = getFileExtension(originalFilename);
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            String uniqueFilename = timestamp + "-" + UUID.randomUUID() + (fileExtension == null || fileExtension.trim().isEmpty() ? "" : "." + fileExtension);
            long nameGenEnd = System.currentTimeMillis();
            System.out.println("[2] 文件名生成耗时: " + (nameGenEnd - nameGenStart) + "ms");

            // 保存文件 - 使用Spring优化的transferTo()方法
            Path filePath = uploadPath.resolve(uniqueFilename);
            System.out.println("[3] 开始写入磁盘...");
            long writeStartTime = System.currentTimeMillis();
            file.transferTo(filePath.toFile());
            long writeEndTime = System.currentTimeMillis();

            long writeDuration = writeEndTime - writeStartTime;
            long totalDuration = writeEndTime - requestStartTime;
            double writeSpeed = fileSize / 1024.0 / 1024.0 / (writeDuration / 1000.0);
            double totalSpeed = fileSize / 1024.0 / 1024.0 / (totalDuration / 1000.0);

            System.out.println("[4] 磁盘写入耗时: " + writeDuration + "ms");
            System.out.println("[5] 磁盘写入速度: " + String.format("%.2f", writeSpeed) + " MB/s");
            System.out.println("[6] 总耗时: " + totalDuration + "ms");
            System.out.println("[7] 整体速度: " + String.format("%.2f", totalSpeed) + " MB/s");
            System.out.println("保存位置: " + filePath.toAbsolutePath());
            System.out.println("===================================\n");

            return new ResponseEntity<>("文件上传成功: " + uniqueFilename +
                    " (耗时: " + totalDuration + "ms, 速度: " + String.format("%.2f", totalSpeed) + "MB/s)",
                    HttpStatus.OK);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity<>("文件上传失败: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String formatFileSize(long size) {
        if (size < 1024) {
            return size + " B";
        } else if (size < 1024 * 1024) {
            return String.format("%.2f KB", size / 1024.0);
        } else if (size < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", size / 1024.0 / 1024.0);
        } else {
            return String.format("%.2f GB", size / 1024.0 / 1024.0 / 1024.0);
        }
    }

    /**
     * 文件下载功能
     */
    @GetMapping("/download")
    public ResponseEntity<InputStreamResource> downloadFile(@RequestParam("name") String fileName) {
        try {
            Path filePath = Paths.get(uploadDir).resolve(fileName);

            // 检查文件是否存在
            if (!Files.exists(filePath)) {
                return ResponseEntity.notFound().build();
            }

            // 获取文件类型
            String contentType = Files.probeContentType(filePath);
            if (contentType == null) {
                contentType = "application/octet-stream";
            }

            // 创建输入流
            InputStream inputStream = Files.newInputStream(filePath);

            // 设置响应头
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + fileName);

            return ResponseEntity
                    .ok()
                    .contentType(MediaType.parseMediaType(contentType))
                    .headers(headers)
                    .body(new InputStreamResource(inputStream));

        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    private String getFileExtension(String filename) {
        if (filename == null || filename.lastIndexOf('.') == -1) {
            return "";
        }
        return filename.substring(filename.lastIndexOf('.') + 1).toLowerCase();
    }
}