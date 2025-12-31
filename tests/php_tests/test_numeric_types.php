<?php
/**
 * Test script to verify that numeric columns are returned as numerics, not strings
 */

$host = '127.0.0.1';
$port = 3306;
$user = 'root';
$pass = 'password';
$db = 'test';

echo "Testing numeric type handling in MiniSQL\n";
echo "========================================\n\n";

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $mysqli = mysqli_init();
    if (defined('MYSQLI_OPT_INT_AND_FLOAT_NATIVE')) {
        $mysqli->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, 1);
    }
    $mysqli->real_connect($host, $user, $pass, $db, $port);
    
    echo "Connected successfully!\n\n";
    
    // Create a test table with various numeric types
    echo "Creating test table...\n";
    // Try to drop if exists, but don't fail if it doesn't
    try {
        $mysqli->query("DROP TABLE IF EXISTS numeric_test");
    } catch (Exception $e) {
        // Ignore if table doesn't exist
    }
    $mysqli->query("CREATE TABLE numeric_test (
        id INTEGER,
        age INTEGER,
        price FLOAT,
        discount FLOAT,
        active BOOLEAN,
        name VARCHAR(50)
    )");
    
    // Insert test data
    echo "Inserting test data...\n";
    $mysqli->query("INSERT INTO numeric_test VALUES (1, 25, 19.99, 0.15, true, 'Alice')");
    $mysqli->query("INSERT INTO numeric_test VALUES (2, 30, 49.50, 0.25, false, 'Bob')");
    
    // Query the data
    echo "Querying data...\n\n";
    $result = $mysqli->query("SELECT * FROM numeric_test");
    
    echo "Results:\n";
    echo "--------\n";
    while ($row = $result->fetch_assoc()) {
        echo "Row data:\n";
        foreach ($row as $key => $value) {
            $type = gettype($value);
            $varType = is_null($value) ? 'NULL' : $type;
            echo "  $key = ";
            var_export($value);
            echo " (type: $varType)\n";
        }
        echo "\n";
    }
    
    // Now test the specific types
    echo "Type verification:\n";
    echo "------------------\n";
    $result = $mysqli->query("SELECT id, age, price, discount, active, name FROM numeric_test WHERE id = 1");
    $row = $result->fetch_assoc();
    
    $tests = [
        ['id', 'integer', is_int($row['id'])],
        ['age', 'integer', is_int($row['age'])],
        ['price', 'double/float', is_float($row['price']) || is_double($row['price'])],
        ['discount', 'double/float', is_float($row['discount']) || is_double($row['discount'])],
        ['active', 'integer (bool)', is_int($row['active'])],
        ['name', 'string', is_string($row['name'])],
    ];
    
    $allPassed = true;
    foreach ($tests as $test) {
        list($column, $expectedType, $passed) = $test;
        $actualType = gettype($row[$column]);
        $status = $passed ? '✓ PASS' : '✗ FAIL';
        echo "$status: $column should be $expectedType, got $actualType\n";
        if (!$passed) {
            $allPassed = false;
        }
    }
    
    echo "\n";
    if ($allPassed) {
        echo "✓ ALL TESTS PASSED! Numeric columns are correctly typed.\n";
    } else {
        echo "✗ SOME TESTS FAILED! Numeric columns are being returned as strings.\n";
    }
    
    // Cleanup
    $mysqli->query("DROP TABLE numeric_test");
    $mysqli->close();
    
} catch (mysqli_sql_exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    echo "Code: " . $e->getCode() . "\n";
}
?>
