<?php
/**
 * Test prepared statements vs regular queries for type handling
 */

$host = '127.0.0.1';
$port = 3306;
$user = 'root';
$pass = 'password';
$db = 'test';

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    echo "Testing Prepared Statements vs Regular Queries\n";
    echo "==============================================\n\n";
    
    // Setup
    try {
        $mysqli->query("DROP TABLE IF EXISTS prep_vs_regular");
    } catch (Exception $e) {}
    
    $mysqli->query("CREATE TABLE prep_vs_regular (
        id INTEGER,
        price FLOAT,
        active BOOLEAN
    )");
    
    $mysqli->query("INSERT INTO prep_vs_regular VALUES (42, 19.99, true)");
    
    // Test 1: Regular query
    echo "Test 1: Regular Query (Text Protocol)\n";
    echo "--------------------------------------\n";
    $result = $mysqli->query("SELECT * FROM prep_vs_regular");
    $row = $result->fetch_assoc();
    
    foreach ($row as $key => $value) {
        echo "$key = ";
        var_export($value);
        echo " (type: " . gettype($value) . ")\n";
    }
    
    echo "\n";
    
    // Test 2: Prepared statement
    echo "Test 2: Prepared Statement (Binary Protocol)\n";
    echo "---------------------------------------------\n";
    $stmt = $mysqli->prepare("SELECT * FROM prep_vs_regular");
    $stmt->execute();
    $result = $stmt->get_result();
    $row = $result->fetch_assoc();
    
    foreach ($row as $key => $value) {
        echo "$key = ";
        var_export($value);
        echo " (type: " . gettype($value) . ")\n";
    }
    
    $stmt->close();
    
    echo "\n";
    echo "CONCLUSION:\n";
    echo "-----------\n";
    echo "Prepared statements use the binary protocol which includes type\n";
    echo "information in a way that mysqli can reliably convert to native types.\n";
    
    // Cleanup
    $mysqli->query("DROP TABLE prep_vs_regular");
    $mysqli->close();
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}
