<?php
/**
 * PHP MySQL connection test script for debugging MiniSQL server
 */

$host = '127.0.0.1';
$port = 3307;  // MiniSQL port (override for local debug)
$user = 'root';
$pass = 'password';
$db = 'test';

echo "Attempting to connect to MiniSQL server at $host:$port\n";

try {
    // Use mysqli with detailed error reporting
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    echo "Connected successfully!\n";
    echo "Server info: " . $mysqli->server_info . "\n";
    
    // Try a simple query
    $result = $mysqli->query("SELECT 1 as test");
    if ($result) {
        $row = $result->fetch_assoc();
        print_r($row);
    }
    
    $mysqli->close();
} catch (mysqli_sql_exception $e) {
    echo "Connection failed: " . $e->getMessage() . "\n";
    echo "Error code: " . $e->getCode() . "\n";
}
?>
