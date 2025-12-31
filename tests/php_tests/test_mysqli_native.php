<?php
/**
 * Test to check MySQLi native type conversion settings
 */

$host = '127.0.0.1';
$port = 3306;
$user = 'root';
$pass = 'password';
$db = 'test';

echo "Testing MySQLi native type conversion\n";
echo "=====================================\n\n";

// Check if mysqlnd is being used
echo "MySQLi client info: " . mysqli_get_client_info() . "\n";
echo "Using mysqlnd: " . (strpos(mysqli_get_client_info(), 'mysqlnd') !== false ? 'YES' : 'NO') . "\n\n";

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    echo "Connected successfully!\n";
    echo "Server info: " . $mysqli->server_info . "\n\n";
    
    // Try setting native types option (may not be available in all PHP versions)
    if (defined('MYSQLI_OPT_INT_AND_FLOAT_NATIVE')) {
        $mysqli->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, 1);
        echo "MYSQLI_OPT_INT_AND_FLOAT_NATIVE is available and set to 1\n\n";
    } else {
        echo "MYSQLI_OPT_INT_AND_FLOAT_NATIVE is NOT available (older PHP version)\n";
        echo "Native type conversion should still work with mysqlnd driver\n\n";
    }
    
    // Create a simple test table
    try {
        $mysqli->query("DROP TABLE IF EXISTS type_test");
    } catch (Exception $e) {}
    
    $mysqli->query("CREATE TABLE type_test (
        num INTEGER,
        price FLOAT,
        active BOOLEAN
    )");
    
    $mysqli->query("INSERT INTO type_test VALUES (42, 3.14, true)");
    
    // Query with fetch_assoc
    echo "Testing fetch_assoc():\n";
    $result = $mysqli->query("SELECT * FROM type_test");
    $row = $result->fetch_assoc();
    
    foreach ($row as $key => $value) {
        echo "  $key = ";
        var_export($value);
        echo " (type: " . gettype($value) . ")\n";
    }
    echo "\n";
    
    // Query with fetch_row (returns indexed array)
    echo "Testing fetch_row():\n";
    $result = $mysqli->query("SELECT * FROM type_test");
    $row = $result->fetch_row();
    
    foreach ($row as $i => $value) {
        echo "  [$i] = ";
        var_export($value);
        echo " (type: " . gettype($value) . ")\n";
    }
    echo "\n";
    
    // Query with fetch_object
    echo "Testing fetch_object():\n";
    $result = $mysqli->query("SELECT * FROM type_test");
    $row = $result->fetch_object();
    
    foreach ($row as $key => $value) {
        echo "  $key = ";
        var_export($value);
        echo " (type: " . gettype($value) . ")\n";
    }
    echo "\n";
    
    // Get field metadata
    echo "Field metadata:\n";
    $result = $mysqli->query("SELECT * FROM type_test");
    $fields = $result->fetch_fields();
    
    foreach ($fields as $field) {
        echo "  {$field->name}:\n";
        echo "    type: {$field->type}\n";
        echo "    flags: {$field->flags}\n";
        echo "    length: {$field->length}\n";
        echo "    charsetnr: {$field->charsetnr}\n";
    }
    
    // Cleanup
    $mysqli->query("DROP TABLE type_test");
    $mysqli->close();
    
} catch (mysqli_sql_exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}
