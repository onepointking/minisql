<?php
/**
 * Test with actual existing tables in the database
 */

$host = '127.0.0.1';
$port = 3307;
$user = 'root';
$pass = 'password';
$db = 'test';

echo "Testing with Existing Tables\n";
echo "=============================\n\n";

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    echo "Connected successfully!\n\n";
    
    // Test with layers table (has integer and string columns)
    echo "Test 1: Query layers table (text protocol)\n";
    echo "-------------------------------------------\n";
    $result = $mysqli->query("SELECT * FROM layers LIMIT 1");
    if ($result && $row = $result->fetch_assoc()) {
        foreach ($row as $key => $value) {
            echo "  $key = " . var_export($value, true) . " (type: " . gettype($value) . ")\n";
        }
    }
    
    echo "\n\nTest 2: Prepared statement on layers (binary protocol)\n";
    echo "-------------------------------------------------------\n";
    $stmt = $mysqli->prepare("SELECT * FROM layers WHERE id = ?");
    $id = 1;
    $stmt->bind_param("i", $id);
    $stmt->execute();
    $result = $stmt->get_result();
    if ($row = $result->fetch_assoc()) {
        foreach ($row as $key => $value) {
            echo "  $key = " . var_export($value, true) . " (type: " . gettype($value) . ")\n";
        }
    }
    $stmt->close();
    
    echo "\n\nTest 3: Query ingest_jobs table (text protocol)\n";
    echo "------------------------------------------------\n";
    $result = $mysqli->query("SELECT * FROM ingest_jobs LIMIT 1");
    if ($result && $row = $result->fetch_assoc()) {
        foreach ($row as $key => $value) {
            $type = gettype($value);
            echo "  $key = " . var_export($value, true) . " (type: $type)\n";
        }
    }
    
    echo "\n\nTest 4: Prepared statement on ingest_jobs (binary protocol)\n";
    echo "------------------------------------------------------------\n";
    $stmt = $mysqli->prepare("SELECT id, organization_id, layer_id, vintage_id FROM ingest_jobs WHERE id = ?");
    $id = 1;
    $stmt->bind_param("i", $id);
    $stmt->execute();
    $result = $stmt->get_result();
    if ($row = $result->fetch_assoc()) {
        foreach ($row as $key => $value) {
            $type = gettype($value);
            echo "  $key = " . var_export($value, true) . " (type: $type)\n";
        }
    }
    $stmt->close();
    
    $mysqli->close();
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    echo "Trace: " . $e->getTraceAsString() . "\n";
}
?>
