<?php
/**
 * Test to inspect field metadata from MiniSQL
 */

$host = '127.0.0.1';
$port = 3307;
$user = 'root';
$pass = 'password';
$db = 'test';

echo "Inspecting field metadata from MiniSQL\n";
echo "======================================\n\n";

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    echo "Connected successfully!\n\n";
    
    // Create a test table
    try {
        $mysqli->query("DROP TABLE IF EXISTS meta_test");
    } catch (Exception $e) {}
    
    $mysqli->query("CREATE TABLE meta_test (
        int_col INTEGER,
        float_col FLOAT,
        bool_col BOOLEAN,
        str_col VARCHAR(50)
    )");
    $mysqli->query("INSERT INTO meta_test VALUES (42, 3.14, true, 'hello')");
    
    // Query and get field metadata
    $result = $mysqli->query("SELECT * FROM meta_test");
    
    echo "Field Metadata:\n";
    echo "---------------\n";
    
    $fields = $result->fetch_fields();
    foreach ($fields as $field) {
        echo "\nColumn: {$field->name}\n";
        echo "  Type: {$field->type} (" . getTypeName($field->type) . ")\n";
        echo "  Flags: {$field->flags} (binary: " . decbin($field->flags) . ")\n";
        echo "  Charset: {$field->charsetnr}\n";
        echo "  Length: {$field->length}\n";
        echo "  Decimals: {$field->decimals}\n";
        
        // Check specific flags
        if ($field->flags & MYSQLI_NUM_FLAG) {
            echo "  ✓ Has NUM_FLAG (0x8000)\n";
        }
        if ($field->flags & MYSQLI_NOT_NULL_FLAG) {
            echo "  ✓ Has NOT_NULL_FLAG\n";
        }
    }
    
    // Now test data retrieval
    echo "\n\nData retrieval:\n";
    echo "---------------\n";
    $result = $mysqli->query("SELECT * FROM meta_test");
    $row = $result->fetch_assoc();
    
    foreach ($row as $key => $value) {
        echo "$key = " . var_export($value, true) . " (PHP type: " . gettype($value) . ")\n";
    }
    
    // Now test WITH native types
    echo "\n\nWith MYSQLI_OPT_INT_AND_FLOAT_NATIVE:\n";
    echo "--------------------------------------\n";
    $mysqli->close();
    
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    $mysqli->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, 1);
    
    $result = $mysqli->query("SELECT * FROM meta_test");
    $row = $result->fetch_assoc();
    
    foreach ($row as $key => $value) {
        echo "$key = " . var_export($value, true) . " (PHP type: " . gettype($value) . ")\n";
    }
    
    $mysqli->query("DROP TABLE meta_test");
    $mysqli->close();
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
}

function getTypeName($type) {
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
        14 => 'NEWDATE',
        15 => 'VARCHAR',
        16 => 'BIT',
        245 => 'JSON',
        246 => 'NEWDECIMAL',
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
    return $types[$type] ?? 'UNKNOWN';
}
?>
