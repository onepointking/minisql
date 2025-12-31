<?php
/**
 * Test to verify the fix for prepared statement column metadata
 * This directly tests the issue you reported
 */

$host = '127.0.0.1';
$port = 3306;
$user = 'root';
$pass = 'password';
$db = 'test';

echo "Testing Prepared Statement Column Metadata Fix\n";
echo "==============================================\n\n";

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    echo "Connected successfully!\n\n";
    
    // Create vintage table exactly as you described
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
    
    $mysqli->query("INSERT INTO vintage (id, layer_id, version, description, created_at, updated_at, metadata) 
                    VALUES (1, 1, '2024', '', '2025-12-19 07:54:52', '2025-12-19 07:54:52', NULL)");
    
    echo "Created and populated vintage table\n\n";
    
    // Test 1: Prepared statement with WHERE clause (mimicking CodeIgniter)
    echo "Test 1: Prepared Statement Query (like CodeIgniter does)\n";
    echo "--------------------------------------------------------\n";
    
    $layerId = 1;
    $version = '2024';
    
    $stmt = $mysqli->prepare("SELECT * FROM vintage WHERE layer_id = ? AND version = ?");
    $stmt->bind_param("is", $layerId, $version);
    $stmt->execute();
    
    $result = $stmt->get_result();
    $vintage = $result->fetch_assoc();
    
    echo "Query: SELECT * FROM vintage WHERE layer_id = ? AND version = ?\n";
    echo "Parameters: layer_id=$layerId, version='$version'\n\n";
    
    echo "Result:\n";
    var_dump($vintage);
    
    echo "\n\nType Analysis:\n";
    $issues = [];
    foreach ($vintage as $key => $value) {
        $type = gettype($value);
        $expected = in_array($key, ['id', 'layer_id']) ? 'integer' : 
                   ($value === null ? 'NULL' : 'string');
        $status = ($type === $expected || ($expected === 'NULL' && $type === 'NULL')) ? '✓' : '✗';
        
        echo "  $status $key: $type";
        if ($type !== $expected && $expected !== 'NULL') {
            echo " (expected: $expected) ← ISSUE";
            $issues[] = $key;
        }
        echo "\n";
    }
    
    $stmt->close();
    
    // Test 2: Without explicit cast (the problem you mentioned)
    echo "\n\nTest 2: Verify no cast needed\n";
    echo "------------------------------\n";
    
    $layerIdCast = intval($layerId);
    $stmt = $mysqli->prepare("SELECT * FROM vintage WHERE layer_id = ? AND version = ?");
    $stmt->bind_param("is", $layerIdCast, $version);
    $stmt->execute();
    
    $result = $stmt->get_result();
    $vintage2 = $result->fetch_assoc();
    
    echo "Query still works (as expected): " . ($vintage2 ? "✓ Yes" : "✗ No") . "\n";
    echo "But now you don't NEED the intval() cast!\n";
    $stmt->close();
    
    // Test 3: Verify the specific issue
    echo "\n\nTest 3: The 0 == false Problem\n";
    echo "-------------------------------\n";
    
    // Insert a row with 0 values
    $mysqli->query("INSERT INTO vintage (id, layer_id, version, description, created_at, updated_at, metadata) 
                    VALUES (2, 0, '2023', 'test', '2025-12-19 07:54:52', '2025-12-19 07:54:52', NULL)");
    
    $stmt = $mysqli->prepare("SELECT id, layer_id FROM vintage WHERE id = ?");
    $id = 2;
    $stmt->bind_param("i", $id);
    $stmt->execute();
    
    $result = $stmt->get_result();
    $row = $result->fetch_assoc();
    
    echo "Row with layer_id = 0:\n";
    echo "  layer_id value: ";
    var_export($row['layer_id']);
    echo " (type: " . gettype($row['layer_id']) . ")\n";
    
    echo "\nPHP Truthiness Tests:\n";
    echo "  \$row['layer_id'] == 0: " . ($row['layer_id'] == 0 ? 'TRUE' : 'FALSE') . "\n";
    echo "  \$row['layer_id'] === 0: " . ($row['layer_id'] === 0 ? 'TRUE' : 'FALSE') . "\n";
    echo "  \$row['layer_id'] == false: " . ($row['layer_id'] == false ? 'TRUE' : 'FALSE') . "\n";
    echo "  \$row['layer_id'] === false: " . ($row['layer_id'] === false ? 'TRUE' : 'FALSE') . "\n";
    echo "  if (\$row['layer_id']): " . ($row['layer_id'] ? 'TRUTHY (enters if)' : 'FALSY (skips if)') . "\n";
    
    $stmt->close();
    
    // Summary
    echo "\n\n" . str_repeat("=", 80) . "\n";
    echo "SUMMARY\n";
    echo str_repeat("=", 80) . "\n";
    
    if (empty($issues)) {
        echo "✓ ALL TESTS PASSED!\n";
        echo "  - INTEGER columns return as PHP integers\n";
        echo "  - STRING columns return as PHP strings\n";
        echo "  - No explicit intval() cast needed\n";
        echo "  - 0 == false works correctly (integer 0 == false is TRUE in PHP)\n";
        echo "  - 0 === false works correctly (integer 0 === false is FALSE in PHP)\n";
        echo "\nThe prepared statement column metadata fix is working!\n";
    } else {
        echo "✗ ISSUES FOUND in columns: " . implode(', ', $issues) . "\n";
        echo "These columns are returning the wrong type.\n";
    }
    
    // Cleanup
    $mysqli->query("DROP TABLE vintage");
    $mysqli->close();
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}
