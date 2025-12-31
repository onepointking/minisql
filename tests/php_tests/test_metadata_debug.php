<?php
/**
 * Debug script to check the exact field metadata being sent
 */

$host = '127.0.0.1';
$port = 3306;
$user = 'root';
$pass = 'password';
$db = 'test';

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    echo "PHP Version: " . PHP_VERSION . "\n";
    echo "MySQLi client: " . mysqli_get_client_info() . "\n\n";
    
    // Create test table
    try {
        $mysqli->query("DROP TABLE IF EXISTS meta_test");
    } catch (Exception $e) {}
    
    $mysqli->query("CREATE TABLE meta_test (
        test_int INTEGER,
        test_float FLOAT,
        test_bool BOOLEAN,
        test_varchar VARCHAR(50)
    )");
    
    $mysqli->query("INSERT INTO meta_test VALUES (42, 3.14, true, 'hello')");
    
    // Get detailed field metadata
    $result = $mysqli->query("SELECT * FROM meta_test");
    $fields = $result->fetch_fields();
    
    echo "Field Metadata:\n";
    echo "===============\n\n";
    
    foreach ($fields as $field) {
        echo "Field: {$field->name}\n";
        echo "  orgname: {$field->orgname}\n";
        echo "  table: {$field->table}\n";
        echo "  orgtable: {$field->orgtable}\n";
        echo "  db: {$field->db}\n";
        echo "  catalog: {$field->catalog}\n";
        echo "  def: " . ($field->def ?? 'NULL') . "\n";
        echo "  max_length: {$field->max_length}\n";
        echo "  length: {$field->length}\n";
        echo "  charsetnr: {$field->charsetnr}\n";
        echo "  flags: {$field->flags} (binary: " . decbin($field->flags) . ")\n";
        echo "  type: {$field->type} (";
        
        // Decode type
        $types = [
            0 => 'DECIMAL',
            1 => 'TINY',
            2 => 'SHORT',
            3 => 'LONG',
            4 => 'FLOAT',
            5 => 'DOUBLE',
            6 => 'NULL',
            7 => 'TIMESTAMP',
            8 => 'LONGLONG',
            9 => 'INT24',
            10 => 'DATE',
            11 => 'TIME',
            12 => 'DATETIME',
            13 => 'YEAR',
            15 => 'VARCHAR',
            16 => 'BIT',
            246 => 'DECIMAL',
            247 => 'ENUM',
            248 => 'SET',
            249 => 'TINY_BLOB',
            250 => 'MEDIUM_BLOB',
            251 => 'LONG_BLOB',
            252 => 'BLOB',
            253 => 'VAR_STRING',
            254 => 'STRING',
            255 => 'GEOMETRY',
        ];
        echo ($types[$field->type] ?? 'UNKNOWN') . ")\n";
        
        // Decode flags
        $flagNames = [];
        if ($field->flags & 1) $flagNames[] = 'NOT_NULL';
        if ($field->flags & 2) $flagNames[] = 'PRI_KEY';
        if ($field->flags & 4) $flagNames[] = 'UNIQUE_KEY';
        if ($field->flags & 8) $flagNames[] = 'MULTIPLE_KEY';
        if ($field->flags & 16) $flagNames[] = 'BLOB';
        if ($field->flags & 32) $flagNames[] = 'UNSIGNED';
        if ($field->flags & 64) $flagNames[] = 'ZEROFILL';
        if ($field->flags & 128) $flagNames[] = 'BINARY';
        if ($field->flags & 256) $flagNames[] = 'ENUM';
        if ($field->flags & 512) $flagNames[] = 'AUTO_INCREMENT';
        if ($field->flags & 1024) $flagNames[] = 'TIMESTAMP';
        if ($field->flags & 2048) $flagNames[] = 'SET';
        if ($field->flags & 4096) $flagNames[] = 'NO_DEFAULT_VALUE';
        if ($field->flags & 8192) $flagNames[] = 'ON_UPDATE_NOW';
        if ($field->flags & 16384) $flagNames[] = 'NUM';
        if ($field->flags & 32768) $flagNames[] = 'NUM';
        
        echo "  flag names: " . (empty($flagNames) ? 'NONE' : implode(', ', $flagNames)) . "\n";
        echo "  decimals: {$field->decimals}\n";
        echo "\n";
    }
    
    // Fetch the actual row data
    echo "\nActual Row Data:\n";
    echo "================\n\n";
    
    $result = $mysqli->query("SELECT * FROM meta_test");
    $row = $result->fetch_assoc();
    
    foreach ($row as $key => $value) {
        echo "$key = ";
        var_export($value);
        echo " (type: " . gettype($value) . ")\n";
    }
    
    // Cleanup
    $mysqli->query("DROP TABLE meta_test");
    $mysqli->close();
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}
