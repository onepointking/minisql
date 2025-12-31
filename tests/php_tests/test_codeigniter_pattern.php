<?php
/**
 * Test CodeIgniter-style query to see actual bytes
 * This mimics the exact pattern you're using
 */

$host = '127.0.0.1';
$port = 3307;  // Will connect through proxy
$user = 'root';
$pass = 'password';
$db = 'test';

echo "CodeIgniter-style Query Test\n";
echo "============================\n\n";

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    echo "Connected to server through proxy on port 3307\n";
    echo "Server info: " . $mysqli->server_info . "\n\n";
    
    // Setup - create a table like your vintage table
    try {
        $mysqli->query("DROP TABLE IF EXISTS vintage");
    } catch (Exception $e) {}
    
    $mysqli->query("CREATE TABLE vintage (
        id INTEGER PRIMARY KEY,
        layer_id INTEGER,
        version VARCHAR(10),
        description TEXT,
        created_at VARCHAR(30),
        updated_at VARCHAR(30),
        metadata TEXT
    )");
    
    echo "Created vintage table\n";
    
    // Insert test data
    $mysqli->query("INSERT INTO vintage (id, layer_id, version, description, created_at, updated_at, metadata) 
                    VALUES (1, 1, '2024', '', '2025-12-19 07:54:52', '2025-12-19 07:54:52', NULL)");
    
    echo "Inserted test data\n\n";
    
    // Now do a query using prepared statement (like CodeIgniter does)
    echo "Executing prepared statement (Binary Protocol):\n";
    echo "Query: SELECT * FROM vintage WHERE layer_id = ? AND version = ?\n\n";
    
    $stmt = $mysqli->prepare("SELECT * FROM vintage WHERE layer_id = ? AND version = ?");
    $layer_id = 1;
    $version = '2024';
    $stmt->bind_param("is", $layer_id, $version);
    $stmt->execute();
    
    $result = $stmt->get_result();
    $row = $result->fetch_assoc();
    
    echo "Result from prepared statement:\n";
    var_dump($row);
    
    echo "\n\nType analysis:\n";
    foreach ($row as $key => $value) {
        $type = gettype($value);
        echo "  $key: $type";
        if ($type === 'string') {
            echo " (expected: " . ($key === 'id' || $key === 'layer_id' ? 'integer' : 'string') . ")";
        }
        echo "\n";
    }
    
    $stmt->close();
    
    // Cleanup
    $mysqli->query("DROP TABLE vintage");
    $mysqli->close();
    
    echo "\n\nDone! Check the proxy output for packet details.\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}
