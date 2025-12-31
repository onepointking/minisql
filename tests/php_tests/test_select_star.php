<?php
// test_select_star.php
// Tests SELECT * behavior and field metadata

function test_select_star($cfg) {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

    $host = $cfg['host'];
    $port = (int)$cfg['port'];
    $user = $cfg['user'];
    $pass = $cfg['pass'];
    $db   = $cfg['db'];

    $table = 'php_layers_'.uniqid();

    try {
        $mysqli = new mysqli($host, $user, $pass, $db, $port);

        // Ensure a clean table
        try { $mysqli->query("DROP TABLE IF EXISTS $table"); } catch (Exception $e) {}

        $mysqli->query("CREATE TABLE $table (name VARCHAR(255), id INT PRIMARY KEY)");
        $mysqli->query("INSERT INTO $table (name, id) VALUES ('aerial', 1)");

        $result = $mysqli->query("SELECT * FROM $table WHERE name = 'aerial'");
        if (!$result) { echo "[FAIL] test_select_star: query failed\n"; return false; }

        // Verify at least one row
        if ($result->num_rows < 1) { echo "[FAIL] test_select_star: no rows\n"; return false; }

        // Basic metadata checks
        $field = $result->fetch_field();
        if (!$field) { echo "[FAIL] test_select_star: missing field metadata\n"; return false; }

        $mysqli->close();
        echo "[PASS] test_select_star\n";
        return true;
    } catch (Exception $e) {
        echo "[FAIL] test_select_star: " . $e->getMessage() . "\n";
        return false;
    }
}

?>
