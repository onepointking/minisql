<?php
// test_column_not_found.php
// Create a small table, then SELECT a non-existent column and assert the MySQL error

function test_column_not_found($cfg) {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

    $host = $cfg['host'];
    $port = (int)$cfg['port'];
    $user = $cfg['user'];
    $pass = $cfg['pass'];
    $db   = $cfg['db'];

    $table = 'php_test_col_'.uniqid();

    try {
        $mysqli = new mysqli($host, $user, $pass, $db, $port);

        // Create a tiny table
        $create_sql = "CREATE TABLE $table (id INT)";
        try {
            $mysqli->query($create_sql);
        } catch (mysqli_sql_exception $e) {
            // If table already exists for some reason, ignore
        }

        // Query a missing column
        $sql = "SELECT missing_column FROM $table";
        $mysqli->query($sql);

        // If we get here, test failed
        echo "[FAIL] test_column_not_found: query unexpectedly succeeded\n";

        // Cleanup
        try { $mysqli->query("DROP TABLE $table"); } catch (Exception $e) {}
        $mysqli->close();
        return false;
    } catch (mysqli_sql_exception $e) {
        // Expected: ER_BAD_FIELD_ERROR (1054)
        $expected_code = 1054;
        $msg = $e->getMessage();
        $code = $e->getCode();

        $expected_fragment = "Unknown column";

        $ok_code = ($code === $expected_code);
        $ok_msg = (stripos($msg, $expected_fragment) !== false);

        // Cleanup - attempt to drop table if exists
        try {
            $mysqli->query("DROP TABLE $table");
        } catch (Exception $ignored) {}

        if ($ok_code && $ok_msg) {
            echo "[PASS] test_column_not_found: code=$code msg=\"$msg\"\n";
            return true;
        } else {
            echo "[FAIL] test_column_not_found: got code=$code msg=\"$msg\" expected code=$expected_code and message containing '$expected_fragment'\n";
            return false;
        }
    } catch (Exception $e) {
        echo "[ERROR] test_column_not_found: unexpected exception: " . $e->getMessage() . "\n";
        return false;
    }
}

?>