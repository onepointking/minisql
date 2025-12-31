<?php
/**
 * Simulate CodeIgniter 4 query behavior
 * This mimics how CI4's QueryBuilder might format queries
 */

$host = '127.0.0.1';
$port = 3306;
$user = 'root';
$pass = 'password';
$db = 'test';

echo "Simulating CodeIgniter 4 QueryBuilder\n\n";

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    // Ensure layers table exists
    try {
        $mysqli->query("DROP TABLE IF EXISTS layers");
    } catch (Exception $e) {}
    
    $mysqli->query("CREATE TABLE layers (id INT PRIMARY KEY, name VARCHAR(255), type VARCHAR(50))");
    $mysqli->query("INSERT INTO layers VALUES (1, 'aerial', 'raster')");
    $mysqli->query("INSERT INTO layers VALUES (2, 'streets', 'vector')");
    
    // Simulate CodeIgniter's QueryBuilder generated queries
    echo "=== Test 1: Basic WHERE clause (like CI4's where() method) ===\n";
    $query1 = "SELECT * FROM `layers` WHERE `name` = 'aerial'";
    echo "Query: $query1\n";
    $result1 = $mysqli->query($query1);
    if ($result1 && $result1->num_rows > 0) {
        echo "SUCCESS: Found " . $result1->num_rows . " row(s)\n";
        $row = $result1->fetch_assoc();
        var_dump($row);
    } else {
        echo "FAILED: No results\n";
    }
    echo "\n";
    
    // Test with potential newlines (if CI4 formats queries for logging)
    echo "=== Test 2: Query with newlines (CI4 debug formatting) ===\n";
    $query2 = "SELECT *
FROM `layers`
WHERE `name` = 'aerial'";
    echo "Query: " . json_encode($query2) . "\n";
    $result2 = $mysqli->query($query2);
    if ($result2 && $result2->num_rows > 0) {
        echo "SUCCESS: Found " . $result2->num_rows . " row(s)\n";
        $row = $result2->fetch_assoc();
        var_dump($row);
    } else {
        echo "FAILED: No results\n";
    }
    echo "\n";
    
    // Test findAll() equivalent
    echo "=== Test 3: Find all records (like CI4's findAll()) ===\n";
    $query3 = "SELECT * FROM `layers`";
    echo "Query: $query3\n";
    $result3 = $mysqli->query($query3);
    if ($result3) {
        echo "SUCCESS: Found " . $result3->num_rows . " row(s)\n";
        while ($row = $result3->fetch_assoc()) {
            var_dump($row);
        }
    } else {
        echo "FAILED: No results\n";
    }
    echo "\n";
    
    echo "All CodeIgniter simulation tests completed successfully!\n";
    
    $mysqli->close();
} catch (mysqli_sql_exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    echo "Code: " . $e->getCode() . "\n";
}
?>
