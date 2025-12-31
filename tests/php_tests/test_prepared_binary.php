<?php
// test_prepared_binary.php
// Verify prepared statements (binary protocol) preserve native types

function test_prepared_binary($cfg) {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

    $host = $cfg['host'];
    $port = (int)$cfg['port'];
    $user = $cfg['user'];
    $pass = $cfg['pass'];
    $db   = $cfg['db'];

    $table = 'prepared_test_'.uniqid();

    try {
        $mysqli = new mysqli($host, $user, $pass, $db, $port);

        try { $mysqli->query("DROP TABLE IF EXISTS $table"); } catch (Exception $e) {}

        $mysqli->query("CREATE TABLE $table (
            id INTEGER,
            age INTEGER,
            price FLOAT,
            active BOOLEAN,
            name VARCHAR(50)
        )");

        $mysqli->query("INSERT INTO $table VALUES (1, 25, 19.99, true, 'Alice')");
        $mysqli->query("INSERT INTO $table VALUES (2, 30, 49.50, false, 'Bob')");
        $mysqli->query("INSERT INTO $table VALUES (3, 35, 99.99, true, 'Charlie')");

        // Prepared statement
        $stmt = $mysqli->prepare("SELECT * FROM $table WHERE id = ?");
        $id = 2; $stmt->bind_param('i', $id); $stmt->execute(); $res = $stmt->get_result(); $row = $res->fetch_assoc();
        if ($row['name'] !== 'Bob') { echo "[FAIL] test_prepared_binary: expected Bob got " . $row['name'] . "\n"; return false; }
        $stmt->close();

        // Type checks
        $stmt = $mysqli->prepare("SELECT id, age, price, active, name FROM $table WHERE id = ?"); $id = 1; $stmt->bind_param('i', $id); $stmt->execute(); $res = $stmt->get_result(); $row = $res->fetch_assoc();
        $tests = [ ['id', 'integer', is_int($row['id'])], ['age','integer', is_int($row['age'])], ['price','float', is_float($row['price']) || is_double($row['price'])], ['active','integer', is_int($row['active'])], ['name','string', is_string($row['name'])] ];
        foreach ($tests as $t) { list($col,$exp,$ok) = $t; if (!$ok) { echo "[FAIL] test_prepared_binary: $col expected $exp got " . gettype($row[$col]) . "\n"; return false; } }

        $mysqli->query("DROP TABLE $table");
        $mysqli->close();
        echo "[PASS] test_prepared_binary\n";
        return true;
    } catch (Exception $e) {
        echo "[FAIL] test_prepared_binary: " . $e->getMessage() . "\n";
        return false;
    }
}

?>
