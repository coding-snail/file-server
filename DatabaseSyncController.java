package com.ys.architecture.execute.controller;

import com.ys.architecture.datasource.DataSourceContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 数据库同步控制器
 * 支持双数据库数据差异对比和迁移（仅处理最近30天数据）
 */
@Slf4j
@RestController
@RequestMapping("/api")
public class DatabaseSyncController {

    // 时间格式
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    @Autowired
    private DataSource dataSource;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 对比两个数据库的数据差异（最近30天）
     * 我     * @param from 源数据库标识（master/backup）
     *
     * @param to 目标数据库标识（master/backup）
     * @return 简化的对比结果
     */
    @GetMapping("/compare")
    public Map<String, Object> compareDatabases(@RequestParam String from, @RequestParam String to) {
        Map<String, Object> result = new HashMap<>();

        try {
            // 验证参数
            if (!isValidDataSource(from) || !isValidDataSource(to)) {
                result.put("success", false);
                result.put("error", "参数错误：from和to必须是'master'或'backup'");
                return result;
            }

            if (from.equals(to)) {
                result.put("success", false);
                result.put("error", "参数错误：from和to不能相同");
                return result;
            }

            // 获取时间范围（最近30天）
            LocalDateTime endTime = LocalDateTime.now();
            LocalDateTime startTime = endTime.minusDays(30);

            // 对比数据差异
            Map<String, Object> dataDifferences = compareDataDifferences(from, to, startTime, endTime);

            // 简化的结果格式
            result.put("success", true);
            result.put("timeRange", getTimeRangeString(startTime, endTime));
            result.put("totalTables", dataDifferences.get("totalTables"));
            result.put("consistentTables", countConsistentTables(dataDifferences));
            result.put("inconsistentTables", countInconsistentTables(dataDifferences));
            result.put("tableDifferences", getSimplifiedTableDifferences(dataDifferences));

        } catch (Exception e) {
            log.error("数据库对比失败", e);
            result.put("success", false);
            result.put("error", "数据库对比失败：" + e.getMessage());
        }

        return result;
    }

    /**
     * 统计一致的表数量
     */
    private int countConsistentTables(Map<String, Object> dataDifferences) {
        Map<String, Object> tableDifferences = (Map<String, Object>) dataDifferences.get("tableDifferences");
        int count = 0;

        for (Object value : tableDifferences.values()) {
            if (value instanceof Map) {
                Map<String, Object> tableDiff = (Map<String, Object>) value;
                Boolean consistent = (Boolean) tableDiff.get("dataConsistent");
                if (consistent != null && consistent) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * 统计不一致的表数量
     */
    private int countInconsistentTables(Map<String, Object> dataDifferences) {
        Map<String, Object> tableDifferences = (Map<String, Object>) dataDifferences.get("tableDifferences");
        int count = 0;

        for (Object value : tableDifferences.values()) {
            if (value instanceof Map) {
                Map<String, Object> tableDiff = (Map<String, Object>) value;
                Boolean consistent = (Boolean) tableDiff.get("dataConsistent");
                if (consistent != null && !consistent) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * 获取简化的表差异信息
     */
    private Map<String, Object> getSimplifiedTableDifferences(Map<String, Object> dataDifferences) {
        Map<String, Object> tableDifferences = (Map<String, Object>) dataDifferences.get("tableDifferences");
        Map<String, Object> simplified = new HashMap<>();

        for (Map.Entry<String, Object> entry : tableDifferences.entrySet()) {
            String tableName = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                Map<String, Object> tableDiff = (Map<String, Object>) value;
                Map<String, Object> simpleDiff = new HashMap<>();

                // 只保留核心信息
                simpleDiff.put("consistent", tableDiff.get("dataConsistent"));
                simpleDiff.put("insertCount", tableDiff.get("insertCount"));
                simpleDiff.put("updateCount", tableDiff.get("updateCount"));
                simpleDiff.put("description", getSimpleDescription(tableDiff));

                simplified.put(tableName, simpleDiff);
            } else {
                // 对比失败的表
                simplified.put(tableName, value);
            }
        }

        return simplified;
    }

    /**
     * 生成简化的描述信息
     */
    private String getSimpleDescription(Map<String, Object> tableDiff) {
        Boolean consistent = (Boolean) tableDiff.get("dataConsistent");
        int insertCount = (Integer) tableDiff.get("insertCount");
        int updateCount = (Integer) tableDiff.get("updateCount");

        if (consistent != null && consistent) {
            return "数据一致";
        }

        StringBuilder desc = new StringBuilder();
        if (insertCount > 0) {
            desc.append("缺失").append(insertCount).append("条");
        }
        if (updateCount > 0) {
            if (desc.length() > 0) desc.append(", ");
            desc.append("需更新").append(updateCount).append("条");
        }

        return desc.toString();
    }

    /**
     * 迁移差异数据（最近30天）
     *
     * @param from 源数据库标识
     * @param to   目标数据库标识
     * @return 迁移结果
     */
    @GetMapping("/migrate")
    public Map<String, Object> migrateData(@RequestParam String from, @RequestParam String to) {
        Map<String, Object> result = new HashMap<>();
        try {
            // 验证参数
            if (!isValidDataSource(from) || !isValidDataSource(to)) {
                result.put("success", false);
                result.put("error", "参数错误：from和to必须是'master'或'backup'");
                return result;
            }

            if (from.equals(to)) {
                result.put("success", false);
                result.put("error", "参数错误：from和to不能相同");
                return result;
            }

            // 获取时间范围（最近30天）
            LocalDateTime endTime = LocalDateTime.now();
            LocalDateTime startTime = endTime.minusDays(30);

            List<String> migratedTables = new ArrayList<>();
            List<String> failedTables = new ArrayList<>();

            // 获取所有表名
            List<String> allTables = getAllTableNames(from);
            for (String table : allTables) {
                try {
                    migrateTableData(from, to, table, startTime, endTime);
                    migratedTables.add(table);
                } catch (Exception e) {
                    log.error("迁移表{}数据失败", table, e);
                    failedTables.add(table + ": " + e.getMessage());
                }
            }
            result.put("success", true);
            result.put("migratedTables", migratedTables);
            result.put("failedTables", failedTables);
            result.put("totalTables", allTables.size());
            result.put("successCount", migratedTables.size());
            result.put("failedCount", failedTables.size());
            result.put("timeRange", getTimeRangeString(startTime, endTime));
        } catch (Exception e) {
            log.error("数据迁移失败", e);
            result.put("success", false);
            result.put("error", "数据迁移失败：" + e.getMessage());
        }

        return result;
    }


    /**
     * 验证数据源参数
     */
    private boolean isValidDataSource(String dataSource) {
        return "master".equals(dataSource) || "backup".equals(dataSource);
    }

    /**
     * 获取所有表名
     */
    private List<String> getAllTableNames(String dataSourceKey) throws SQLException {
        String originalDataSource = DataSourceContextHolder.getDataSource();
        try {
            DataSourceContextHolder.setDataSource(dataSourceKey);

            List<String> tables = new ArrayList<>();
            try (Connection conn = this.dataSource.getConnection()) {
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet rs = metaData.getTables("architecture_tool", "architecture_tool", "%", new String[]{"TABLE"});
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
            return tables;
        } finally {
            DataSourceContextHolder.setDataSource(originalDataSource);
        }
    }

    /**
     * 对比数据差异
     */
    private Map<String, Object> compareDataDifferences(String from, String to, LocalDateTime startTime, LocalDateTime endTime) {
        Map<String, Object> result = new HashMap<>();
        List<String> commonTables;
        try {
            commonTables = getCommonTables(from, to);
        } catch (SQLException e) {
            log.error("获取共同表失败", e);
            result.put("error", "获取共同表失败：" + e.getMessage());
            return result;
        }

        Map<String, Object> tableDifferences = new HashMap<>();

        for (String table : commonTables) {
            try {
                Map<String, Object> tableDiff = compareTableData(from, to, table, startTime, endTime);
                tableDifferences.put(table, tableDiff);
            } catch (Exception e) {
                log.error("对比表{}数据失败", table, e);
                tableDifferences.put(table, "对比失败：" + e.getMessage());
            }
        }

        result.put("tableDifferences", tableDifferences);
        result.put("totalTables", commonTables.size());
        return result;
    }

    /**
     * 获取两个数据库的共同表
     */
    private List<String> getCommonTables(String from, String to) throws SQLException {
        List<String> fromTables = getAllTableNames(from);
        List<String> toTables = getAllTableNames(to);

        List<String> commonTables = new ArrayList<>(fromTables);
        commonTables.retainAll(toTables);
        return commonTables;
    }

    /**
     * 对比表数据差异（修复版：确保数据一致性）
     */
    private Map<String, Object> compareTableData(String from, String to, String tableName, LocalDateTime startTime, LocalDateTime endTime) {
        Map<String, Object> result = new HashMap<>();

        String originalDataSource = DataSourceContextHolder.getDataSource();

        try {
            // 构建时间范围查询条件（源库和目标库使用相同的时间范围）
            String timeCondition = buildTimeCondition(tableName, startTime, endTime);

            // 从源数据库读取指定时间范围的数据
            DataSourceContextHolder.setDataSource(from);
            List<Map<String, Object>> fromData = jdbcTemplate.queryForList("SELECT * FROM " + tableName + timeCondition);

            // 从目标数据库读取相同时间范围的数据
            DataSourceContextHolder.setDataSource(to);
            List<Map<String, Object>> toData = jdbcTemplate.queryForList("SELECT * FROM " + tableName + timeCondition);

            // 按主键对比差异
            String primaryKey = getPrimaryKeyForTable(tableName);
            Map<String, Object> diffResult = compareDataByPrimaryKeyForMerge(fromData, toData, primaryKey);

            result.putAll(diffResult);
            result.put("sourceDataCount", fromData.size());
            result.put("targetDataCount", toData.size());

            // 添加更清晰的统计信息
            int insertCount = (Integer) diffResult.get("insertCount");
            int updateCount = (Integer) diffResult.get("updateCount");
            result.put("mergeOperationCount", insertCount + updateCount);
            result.put("mergeDescription", getMergeDescription(diffResult));

            // 添加调试信息
            result.put("debug", "对比时间范围：" + timeCondition);

        } catch (Exception e) {
            throw new RuntimeException("对比表" + tableName + "数据失败：" + e.getMessage(), e);
        } finally {
            DataSourceContextHolder.setDataSource(originalDataSource);
        }

        return result;
    }

    /**
     * 生成合并操作的描述信息
     */
    private String getMergeDescription(Map<String, Object> diffResult) {
        int insertCount = (int) diffResult.get("insertCount");
        int updateCount = (int) diffResult.get("updateCount");
        int unchangedCount = (int) diffResult.get("unchangedCount");
        int targetOnlyCount = (int) diffResult.get("targetOnlyCount");

        StringBuilder desc = new StringBuilder();
        if (insertCount > 0) {
            desc.append("需要插入").append(insertCount).append("条记录");
        }
        if (updateCount > 0) {
            if (desc.length() > 0) desc.append(", ");
            desc.append("需要更新").append(updateCount).append("条记录");
        }
        if (unchangedCount > 0) {
            if (desc.length() > 0) desc.append(", ");
            desc.append("保持不变").append(unchangedCount).append("条记录");
        }
        if (targetOnlyCount > 0) {
            if (desc.length() > 0) desc.append(", ");
            desc.append("目标库独有").append(targetOnlyCount).append("条记录");
        }

        if (desc.length() == 0) {
            desc.append("无数据需要合并");
        }

        return desc.toString();
    }

    /**
     * 检查两行数据是否相等（修复版：处理MySQL时间戳和空值）
     */
    private boolean isRowEqual(Map<String, Object> row1, Map<String, Object> row2) {
        if (row1.size() != row2.size()) {
            return false;
        }

        for (Map.Entry<String, Object> entry : row1.entrySet()) {
            String key = entry.getKey();
            Object value1 = entry.getValue();
            Object value2 = row2.get(key);

            // 处理空值
            if (value1 == null && value2 == null) {
                continue;
            }
            if (value1 == null || value2 == null) {
                return false;
            }

            // 处理时间戳字段（MySQL timestamp可能包含毫秒差异）
            if (isTimestampField(key)) {
                if (!isTimestampEqual(value1, value2)) {
                    return false;
                }
            } else {
                // 普通字段比较
                if (!value1.equals(value2)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 判断是否为时间戳字段
     */
    private boolean isTimestampField(String columnName) {
        return columnName.toLowerCase().contains("time") || columnName.toLowerCase().contains("date") || columnName.toLowerCase().contains("timestamp");
    }

    /**
     * 比较时间戳是否相等（忽略毫秒差异）
     */
    private boolean isTimestampEqual(Object timestamp1, Object timestamp2) {
        try {
            // 如果是java.sql.Timestamp或java.util.Date，比较到秒级精度
            if (timestamp1 instanceof java.sql.Timestamp && timestamp2 instanceof java.sql.Timestamp) {
                java.sql.Timestamp ts1 = (java.sql.Timestamp) timestamp1;
                java.sql.Timestamp ts2 = (java.sql.Timestamp) timestamp2;
                return Math.abs(ts1.getTime() - ts2.getTime()) < 1000; // 1秒内的差异视为相等
            }

            // 如果是字符串形式的时间戳
            if (timestamp1 instanceof String && timestamp2 instanceof String) {
                String str1 = ((String) timestamp1).replaceAll("\\.\\d+", ""); // 移除毫秒部分
                String str2 = ((String) timestamp2).replaceAll("\\.\\d+", "");
                return str1.equals(str2);
            }

            // 默认比较
            return timestamp1.equals(timestamp2);
        } catch (Exception e) {
            // 如果比较失败，使用默认比较
            return timestamp1.equals(timestamp2);
        }
    }

    /**
     * 按主键对比数据差异（针对合并场景）
     */
    private Map<String, Object> compareDataByPrimaryKeyForMerge(List<Map<String, Object>> fromData, List<Map<String, Object>> toData, String primaryKey) {
        Map<String, Object> result = new HashMap<>();

        // 构建主键映射
        Map<Object, Map<String, Object>> fromMap = fromData.stream().collect(Collectors.toMap(row -> row.get(primaryKey), row -> row));

        Map<Object, Map<String, Object>> toMap = toData.stream().collect(Collectors.toMap(row -> row.get(primaryKey), row -> row));

        // 找出仅在源库存在的数据（需要插入）
        List<Object> insertKeys = fromMap.keySet().stream().filter(key -> !toMap.containsKey(key)).collect(Collectors.toList());

        // 找出共同主键但数据不同的记录（需要更新）
        List<Object> updateKeys = new ArrayList<>();
        // 找出共同主键且数据相同的记录（保持不变）
        List<Object> unchangedKeys = new ArrayList<>();

        for (Object key : fromMap.keySet()) {
            if (toMap.containsKey(key)) {
                Map<String, Object> fromRow = fromMap.get(key);
                Map<String, Object> toRow = toMap.get(key);
                if (isRowEqual(fromRow, toRow)) {
                    unchangedKeys.add(key);
                } else {
                    updateKeys.add(key);
                }
            }
        }

        // 找出仅在目标库存在的数据（目标库独有，不参与合并）
        List<Object> targetOnlyKeys = toMap.keySet().stream().filter(key -> !fromMap.containsKey(key)).collect(Collectors.toList());

        result.put("insertCount", insertKeys.size());
        result.put("updateCount", updateKeys.size());
        result.put("unchangedCount", unchangedKeys.size());
        result.put("targetOnlyCount", targetOnlyKeys.size());
        result.put("dataConsistent", insertKeys.isEmpty() && updateKeys.isEmpty());

        return result;
    }

    /**
     * 根据表名获取主键字段
     */
    private String getPrimaryKeyForTable(String tableName) {
        // 根据表名返回对应的主键字段
        switch (tableName.toLowerCase()) {
            case "task":
            case "task_element":
            case "region_module_config":
                return "id";
            default:
                // 默认使用id作为主键
                return "id";
        }
    }

    /**
     * 获取表的列名（修复版：添加调试和过滤，排除虚拟生成列）
     */
    private List<String> getTableColumnNames(String dataSourceKey, String tableName) throws SQLException {
        String originalDataSource = DataSourceContextHolder.getDataSource();
        try {
            DataSourceContextHolder.setDataSource(dataSourceKey);

            List<String> columns = new ArrayList<>();
            try (Connection conn = this.dataSource.getConnection()) {
                // 使用 INFORMATION_SCHEMA 查询，过滤掉虚拟生成列
                String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                        "AND (GENERATION_EXPRESSION IS NULL OR GENERATION_EXPRESSION = '') " +
                        "ORDER BY ORDINAL_POSITION";

                try (java.sql.PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, "architecture_tool");
                    stmt.setString(2, tableName);

                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            columns.add(rs.getString("COLUMN_NAME"));
                        }
                    }
                }

                log.debug("表{}的非生成列：{}", tableName, columns);
            }
            return columns;
        } finally {
            DataSourceContextHolder.setDataSource(originalDataSource);
        }
    }

    /**
     * 迁移表数据（修复版：处理TEXT/BLOB字段类型）
     */
    private void migrateTableData(String from, String to, String tableName, LocalDateTime startTime, LocalDateTime endTime) {
        String originalDataSource = DataSourceContextHolder.getDataSource();

        try {
            // 构建时间范围查询条件（只查询源库的时间范围）
            String timeCondition = buildTimeCondition(tableName, startTime, endTime);

            // 从源数据库读取指定时间范围的数据
            DataSourceContextHolder.setDataSource(from);
            List<Map<String, Object>> fromData = jdbcTemplate.queryForList("SELECT * FROM " + tableName + timeCondition);

            if (fromData.isEmpty()) {
                log.info("表{}无数据需要迁移", tableName);
                return;
            }

            // 获取目标数据库数据（不限制时间范围，因为需要检查所有主键）
            DataSourceContextHolder.setDataSource(to);
            List<Map<String, Object>> toData = jdbcTemplate.queryForList("SELECT * FROM " + tableName);

            // 按主键对比差异
            String primaryKey = getPrimaryKeyForTable(tableName);

            // 获取表结构信息
            List<String> columns = getTableColumnNames(from, tableName);

            // 验证并过滤有效的表字段
            columns = validateAndFilterColumns(tableName, columns, fromData);

            // 构建插入和更新SQL
            String insertSql = buildInsertSql(tableName, columns);
            String updateSql = buildUpdateSql(tableName, columns, primaryKey);

            // 构建目标库主键集合（用于快速查找）
            Set<Object> toPrimaryKeys = toData.stream().map(row -> row.get(primaryKey)).collect(Collectors.toSet());

            // 执行合并迁移
            int insertCount = 0;
            int updateCount = 0;

            for (Map<String, Object> fromRow : fromData) {
                Object primaryKeyValue = fromRow.get(primaryKey);

                if (toPrimaryKeys.contains(primaryKeyValue)) {
                    // 目标库已存在该主键：执行UPDATE操作
                    // 构建UPDATE参数：所有列（除了主键）+ 主键值
                    List<Object> updateParams = columns.stream()
                            .filter(col -> !col.equals(primaryKey))
                            .map(col -> convertBlobData(fromRow.get(col))) // 转换BLOB/TEXT数据
                            .collect(Collectors.toList());
                    updateParams.add(primaryKeyValue);

                    jdbcTemplate.update(updateSql, updateParams.toArray());
                    updateCount++;
                } else {
                    // 目标库不存在该主键：执行INSERT操作
                    Object[] params = columns.stream()
                            .map(col -> convertBlobData(fromRow.get(col))) // 转换BLOB/TEXT数据
                            .toArray();
                    jdbcTemplate.update(insertSql, params);
                    insertCount++;
                }
            }

            log.info("成功合并表{}：插入{}条新记录，更新{}条现有记录", tableName, insertCount, updateCount);

        } catch (Exception e) {
            throw new RuntimeException("合并表" + tableName + "数据失败：" + e.getMessage(), e);
        } finally {
            DataSourceContextHolder.setDataSource(originalDataSource);
        }
    }

    /**
     * 转换BLOB/TEXT/时间类型数据，避免类型转换错误
     */
    private Object convertBlobData(Object data) {
        if (data == null) {
            return null;
        }

        // 如果是BLOB或TEXT类型的数据，转换为合适的类型
        if (data instanceof java.sql.Blob) {
            try {
                java.sql.Blob blob = (java.sql.Blob) data;
                return blob.getBytes(1, (int) blob.length());
            } catch (Exception e) {
                log.warn("BLOB数据转换失败，使用原始数据", e);
                return data;
            }
        }

        // 如果是Clob类型（某些JDBC驱动会将TEXT类型读取为Clob）
        if (data instanceof java.sql.Clob) {
            try {
                java.sql.Clob clob = (java.sql.Clob) data;
                return clob.getSubString(1, (int) clob.length());
            } catch (Exception e) {
                log.warn("CLOB数据转换失败，使用原始数据", e);
                return data;
            }
        }

        // 如果是LocalDateTime，转换为Timestamp（确保与MySQL的timestamp类型兼容）
        if (data instanceof java.time.LocalDateTime) {
            return java.sql.Timestamp.valueOf((java.time.LocalDateTime) data);
        }

        // 如果是java.util.Date但不是java.sql.Timestamp，转换为Timestamp
        if (data instanceof java.util.Date && !(data instanceof java.sql.Timestamp)) {
            return new java.sql.Timestamp(((java.util.Date) data).getTime());
        }

        // 如果是byte数组，直接返回
        if (data instanceof byte[]) {
            return data;
        }

        // 其他类型直接返回（包括String、Integer、Long、java.sql.Timestamp等）
        return data;
    }

    /**
     * 构建插入SQL
     */
    private String buildInsertSql(String tableName, List<String> columns) {
        String columnList = String.join(", ", columns);
        String placeholders = columns.stream().map(col -> "?").collect(Collectors.joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columnList, placeholders);
    }

    /**
     * 构建更新SQL
     */
    private String buildUpdateSql(String tableName, List<String> columns, String primaryKey) {
        String setClause = columns.stream().filter(col -> !col.equals(primaryKey)) // 排除主键字段
                .map(col -> col + " = ?").collect(Collectors.joining(", "));
        return String.format("UPDATE %s SET %s WHERE %s = ?", tableName, setClause, primaryKey);
    }

    /**
     * 验证并过滤有效的表字段
     */
    private List<String> validateAndFilterColumns(String tableName, List<String> columns, List<Map<String, Object>> sampleData) {
        if (sampleData.isEmpty()) {
            return columns;
        }

        // 从实际数据中获取存在的字段
        Set<String> actualColumns = sampleData.get(0).keySet();

        // 只保留在实际数据中存在的字段
        List<String> validColumns = columns.stream().filter(actualColumns::contains).collect(Collectors.toList());

        if (validColumns.size() != columns.size()) {
            log.warn("表{}检测到不一致的字段定义。数据库元数据字段：{}，实际数据字段：{}", tableName, columns, actualColumns);
            log.warn("将使用实际数据中的字段：{}", validColumns);
        }

        return validColumns;
    }

    /**
     * 构建时间范围查询条件
     */
    private String buildTimeCondition(String tableName, LocalDateTime startTime, LocalDateTime endTime) {
        // 根据表名确定时间字段
        String timeColumn = getTimeColumnForTable(tableName);

        // 如果表没有时间字段，不添加时间条件
        if (timeColumn == null) {
            log.warn("表{}没有配置时间字段，将迁移所有数据", tableName);
            return "";
        }

        String startStr = startTime.format(DATE_FORMATTER);
        String endStr = endTime.format(DATE_FORMATTER);
        return " WHERE " + timeColumn + " >= '" + startStr + "' AND " + timeColumn + " <= '" + endStr + "'";
    }

    /**
     * 根据表名确定时间字段
     */
    private String getTimeColumnForTable(String tableName) {
        // 根据表名返回对应的时间字段
        switch (tableName.toLowerCase()) {
            case "core_config":
            case "region_module_config":
            case "task":
            case "task_element":
                return "created_time";
            default:
                return "update_time";
        }
    }


    /**
     * 比较单个字段的值
     */
    private boolean isFieldEqual(Object value1, Object value2) {
        if (value1 == null && value2 == null) {
            return true;
        }
        if (value1 == null || value2 == null) {
            return false;
        }

        // 处理时间戳字段
        if (value1 instanceof java.sql.Timestamp && value2 instanceof java.sql.Timestamp) {
            return isTimestampEqual(value1, value2);
        }

        return value1.equals(value2);
    }

    /**
     * 获取时间范围字符串
     */
    private String getTimeRangeString(LocalDateTime startTime, LocalDateTime endTime) {
        return startTime.format(DATE_FORMATTER) + " 至 " + endTime.format(DATE_FORMATTER);
    }
}