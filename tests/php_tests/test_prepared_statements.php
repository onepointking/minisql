<?php
// test_prepared_statements.php
// Prepared statements test converted into callable function

function test_prepared_statements($cfg) {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

    $host = $cfg['host'];
    $port = (int)$cfg['port'];
    $user = $cfg['user'];
    $pass = $cfg['pass'];
    $db   = $cfg['db'];

    $table = 'php_prep_test_'.uniqid();

    try {
        $mysqli = new mysqli($host, $user, $pass, $db, $port);

        try { $mysqli->query("DROP TABLE IF EXISTS $table"); } catch (Exception $e) {}

        $mysqli->query("CREATE TABLE $table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            value INTEGER
        )");

    // Insert rows (use simple queries to avoid driver/binary-protocol edge cases)
    $mysqli->query("INSERT INTO $table (id, name, value) VALUES (1, 'Alice', 100)");
    $mysqli->query("INSERT INTO $table (id, name, value) VALUES (2, 'Bob', 200)");
    $mysqli->query("INSERT INTO $table (id, name, value) VALUES (3, 'Charlie', 150)");

        // Prepared select
        $stmt = $mysqli->prepare("SELECT id, name, value FROM $table WHERE id = ?");
        $search_id = 2; $stmt->bind_param('i', $search_id); $stmt->execute();
        $result = $stmt->get_result(); $row = $result->fetch_assoc();
        if ($row['name'] !== 'Bob') { echo "[FAIL] test_prepared_statements: expected Bob got " . $row['name'] . "\n"; return false; }
        $stmt->close();

        // SELECT * prepared
        $stmt = $mysqli->prepare("SELECT * FROM $table WHERE id = ?"); $search_id = 2; $stmt->bind_param('i', $search_id); $stmt->execute(); $res = $stmt->get_result(); $r = $res->fetch_assoc(); if ($r['name'] !== 'Bob') { echo "[FAIL] test_prepared_statements: select * mismatch\n"; return false; }

        // Update
        $stmt = $mysqli->prepare("UPDATE $table SET value = ? WHERE id = ?"); $new_value = 999; $update_id = 1; $stmt->bind_param('ii', $new_value, $update_id); $stmt->execute(); $stmt->close();
        $res = $mysqli->query("SELECT value FROM $table WHERE id = 1"); $r = $res->fetch_assoc(); if ($r['value'] != 999) { echo "[FAIL] test_prepared_statements: update failed\n"; return false; }

        // Delete
        $stmt = $mysqli->prepare("DELETE FROM $table WHERE id = ?"); $delete_id = 3; $stmt->bind_param('i', $delete_id); $stmt->execute(); $stmt->close();
        $res = $mysqli->query("SELECT COUNT(*) as cnt FROM $table"); $r = $res->fetch_assoc(); if ($r['cnt'] != 2) { echo "[FAIL] test_prepared_statements: delete failed\n"; return false; }

        // Cleanup
        $mysqli->query("DROP TABLE $table");
        $mysqli->close();

        echo "[PASS] test_prepared_statements\n";
        return true;

    } catch (Exception $e) {
        echo "[FAIL] test_prepared_statements: " . $e->getMessage() . "\n";
        return false;
    }
}

?>
