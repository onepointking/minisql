<?php
// test_last_insert_id.php
// Verify mysqli->insert_id and LAST_INSERT_ID() behavior for AUTO_INCREMENT inserts

function test_last_insert_id($cfg) {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

    $host = $cfg['host'];
    $port = (int)$cfg['port'];
    $user = $cfg['user'];
    $pass = $cfg['pass'];
    $db   = $cfg['db'];

    $table = 'php_test_lastid_'.uniqid();

    try {
        $mysqli = new mysqli($host, $user, $pass, $db, $port);

        // Create table with AUTO_INCREMENT primary key
        $create_sql = "CREATE TABLE $table (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)";
        try {
            $mysqli->query($create_sql);
        } catch (mysqli_sql_exception $e) {
            // ignore
        }

        // First insert (no id specified)
        $mysqli->query("INSERT INTO $table (name) VALUES ('first')");
        $id1 = $mysqli->insert_id;
        if (!is_int($id1) && !ctype_digit((string)$id1)) {
            echo "[FAIL] test_last_insert_id: first insert produced non-integer insert_id: $id1\n";
            return false;
        }

        // SELECT LAST_INSERT_ID() should match mysqli->insert_id
        $res = $mysqli->query("SELECT LAST_INSERT_ID() AS lastid");
        $row = $res->fetch_assoc();
        $lastid = (int)$row['lastid'];
        if ($lastid !== (int)$id1) {
            echo "[FAIL] test_last_insert_id: LAST_INSERT_ID()=$lastid does not match mysqli->insert_id=$id1\n";
            return false;
        }

        // Second insert
        $mysqli->query("INSERT INTO $table (name) VALUES ('second')");
        $id2 = $mysqli->insert_id;
        if ((int)$id2 !== ((int)$id1 + 1)) {
            echo "[FAIL] test_last_insert_id: second insert id expected " . ((int)$id1 + 1) . " got $id2\n";
            return false;
        }

        // Third insert via prepared statement
        $stmt = $mysqli->prepare("INSERT INTO $table (name) VALUES (?)");
        $name = 'third';
        $stmt->bind_param('s', $name);
        $stmt->execute();
        $id3 = $mysqli->insert_id;
        if ((int)$id3 !== ((int)$id2 + 1)) {
            echo "[FAIL] test_last_insert_id: third insert id expected " . ((int)$id2 + 1) . " got $id3\n";
            return false;
        }

        // Cleanup
        try { $mysqli->query("DROP TABLE $table"); } catch (Exception $e) {}
        $mysqli->close();

        echo "[PASS] test_last_insert_id: ids = $id1, $id2, $id3\n";
        return true;

    } catch (mysqli_sql_exception $e) {
        echo "[FAIL] test_last_insert_id: unexpected SQL exception: " . $e->getMessage() . "\n";
        return false;
    } catch (Exception $e) {
        echo "[ERROR] test_last_insert_id: unexpected exception: " . $e->getMessage() . "\n";
        return false;
    }
}

?>
