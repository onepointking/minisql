<?php
/**
 * Test to see if mysqli returns native types by default
 * with a real MySQL server
 */

echo "Testing mysqli default behavior with standard MySQL\n";
echo "===================================================\n\n";

// Connect to a real MySQL if available (adjust as needed)
$realHost = '127.0.0.1';
$realPort = 3306;  // Standard MySQL port
$user = 'root';
$pass = '';
$db = 'test';

// First test with real MySQL if available
try {
    $mysqli = new mysqli($realHost, $user, $pass, $db, $realPort);
    echo "Connected to real MySQL\n";
    
    $mysqli->query("DROP TABLE IF EXISTS type_test");
    $mysqli->query("CREATE TABLE type_test (id INT, val FLOAT, name VARCHAR(50))");
    $mysqli->query("INSERT INTO type_test VALUES (123, 45.67, 'test')");
    
    $result = $mysqli->query("SELECT * FROM type_test");
    $row = $result->fetch_assoc();
    
    echo "\nReal MySQL results WITHOUT MYSQLI_OPT_INT_AND_FLOAT_NATIVE:\n";
    foreach ($row as $key => $value) {
        echo "  $key = " . var_export($value, true) . " (type: " . gettype($value) . ")\n";
    }
    
    $mysqli->query("DROP TABLE type_test");
    $mysqli->close();
    
} catch (Exception $e) {
    echo "Could not connect to real MySQL (this is OK for testing): " . $e->getMessage() . "\n";
}

echo "\n";
?>
