<?php
/**
 * Test edge cases to ensure FROM detection works correctly
 */

$host = '127.0.0.1';
$port = 3306;
$user = 'root';
$pass = 'password';
$db = 'test';

echo "Testing FROM detection edge cases\n\n";

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    // Create test table
    try {
        $mysqli->query("DROP TABLE IF EXISTS test_table");
    } catch (mysqli_sql_exception $e) {
        // Ignore if table doesn't exist
    }
    $mysqli->query("CREATE TABLE test_table (id INT PRIMARY KEY, platform VARCHAR(255), inform VARCHAR(255))");
    $mysqli->query("INSERT INTO test_table VALUES (1, 'linux', 'data')");
    
    $tests = [
        // These should query the table
        "SELECT * FROM test_table" => "Should return data from table",
        "SELECT id FROM test_table" => "Should return id column",
        "SELECT platform FROM test_table" => "Should return platform column (contains FROM in name)",
        "SELECT inform FROM test_table" => "Should return inform column (contains FROM in name)",
        
        // These should be treated as simple expressions (no table access)
        "SELECT 'platform'" => "Should return literal string",
        "SELECT 'inform'" => "Should return literal string",
    ];
    
    foreach ($tests as $query => $description) {
        echo "Test: $description\n";
        echo "Query: $query\n";
        try {
            $result = $mysqli->query($query);
            if ($result) {
                $row = $result->fetch_assoc();
                echo "Result: ";
                var_dump($row);
            }
        } catch (mysqli_sql_exception $e) {
            echo "Error: " . $e->getMessage() . "\n";
        }
        echo "\n";
    }
    
    $mysqli->close();
} catch (mysqli_sql_exception $e) {
    echo "Connection/Setup error: " . $e->getMessage() . "\n";
}
?>
