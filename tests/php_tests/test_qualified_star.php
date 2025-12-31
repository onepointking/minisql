<?php
/**
 * Test qualified star (table.*) syntax
 */

$mysqli = new mysqli('127.0.0.1', 'root', '', 'test', 3306);

if ($mysqli->connect_error) {
    die('Connect Error: ' . $mysqli->connect_error);
}

echo "Connected successfully\n\n";

// Create test table
echo "Creating test table...\n";
// $mysqli->query("DROP TABLE IF EXISTS test_users");
if (!$mysqli->query("CREATE TABLE IF NOT EXISTS test_users (id INTEGER PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))")) {
    die("Failed to create table: " . $mysqli->error . "\n");
}
// Clear existing data
$mysqli->query("DELETE FROM test_users");

// Insert test data
echo "Inserting test data...\n";
$mysqli->query("INSERT INTO test_users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
$mysqli->query("INSERT INTO test_users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')");

// Test 1: Simple qualified star
echo "\n=== Test 1: SELECT test_users.* FROM test_users ===\n";
$result = $mysqli->query("SELECT test_users.* FROM test_users");
if ($result) {
    while ($row = $result->fetch_assoc()) {
        echo "ID: {$row['id']}, Name: {$row['name']}, Email: {$row['email']}\n";
    }
    $result->free();
} else {
    echo "ERROR: " . $mysqli->error . "\n";
}

// Test 2: Qualified star with table alias
echo "\n=== Test 2: SELECT u.* FROM test_users u ===\n";
$result = $mysqli->query("SELECT u.* FROM test_users u");
if ($result) {
    while ($row = $result->fetch_assoc()) {
        echo "ID: {$row['id']}, Name: {$row['name']}, Email: {$row['email']}\n";
    }
    $result->free();
} else {
    echo "ERROR: " . $mysqli->error . "\n";
}

// Test 3: Mixed columns with qualified star
echo "\n=== Test 3: SELECT id, test_users.*, name FROM test_users ===\n";
$result = $mysqli->query("SELECT id, test_users.*, name FROM test_users");
if ($result) {
    $row = $result->fetch_assoc();
    echo "Columns returned: " . implode(", ", array_keys($row)) . "\n";
    do {
        echo "Row: " . json_encode($row) . "\n";
    } while ($row = $result->fetch_assoc());
    $result->free();
} else {
    echo "ERROR: " . $mysqli->error . "\n";
}

// Test 4: Simple star for comparison
echo "\n=== Test 4: SELECT * FROM test_users (for comparison) ===\n";
$result = $mysqli->query("SELECT * FROM test_users");
if ($result) {
    while ($row = $result->fetch_assoc()) {
        echo "ID: {$row['id']}, Name: {$row['name']}, Email: {$row['email']}\n";
    }
    $result->free();
} else {
    echo "ERROR: " . $mysqli->error . "\n";
}

// Create second table for JOIN test
echo "\n=== Setting up JOIN test ===\n";
// $mysqli->query("DROP TABLE IF EXISTS test_orders");
$mysqli->query("CREATE TABLE IF NOT EXISTS test_orders (order_id INTEGER PRIMARY KEY, user_id INTEGER, product VARCHAR(100))");
$mysqli->query("DELETE FROM test_orders");
$mysqli->query("INSERT INTO test_orders (order_id, user_id, product) VALUES (101, 1, 'Widget')");
$mysqli->query("INSERT INTO test_orders (order_id, user_id, product) VALUES (102, 2, 'Gadget')");

// Test 5: Qualified star with JOIN
echo "\n=== Test 5: SELECT u.*, o.* FROM test_users u JOIN test_orders o ON u.id = o.user_id ===\n";
$result = $mysqli->query("SELECT u.*, o.* FROM test_users u JOIN test_orders o ON u.id = o.user_id");
if ($result) {
    while ($row = $result->fetch_assoc()) {
        echo json_encode($row) . "\n";
    }
    $result->free();
} else {
    echo "ERROR: " . $mysqli->error . "\n";
}

echo "\n=== All tests completed ===\n";

$mysqli->close();
