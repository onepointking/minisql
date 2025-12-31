<?php
// test_table_not_found.php
// Attempt to SELECT from a non-existent table and assert we receive the correct MySQL error

function test_table_not_found($cfg) {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

    $host = $cfg['host'];
    $port = (int)$cfg['port'];
    $user = $cfg['user'];
    $pass = $cfg['pass'];
    $db   = $cfg['db'];

    $table = 'this_table_does_not_exist_'.uniqid();

    try {
        $mysqli = new mysqli($host, $user, $pass, $db, $port);

        // Run the query that should fail
        $sql = "SELECT * FROM $table";
        $mysqli->query($sql);

        // If we get here, that's a failure for this test
        echo "[FAIL] test_table_not_found: query unexpectedly succeeded\n";
        $mysqli->close();
        return false;
    } catch (mysqli_sql_exception $e) {
        // Expected: ER_NO_SUCH_TABLE (1146)
        $expected_code = 1146;
        $msg = $e->getMessage();
        $code = $e->getCode();

        // Message should mention doesn't exist and include database.table form
        $expected_table_fragment = "doesn't exist";

        $ok_code = ($code === $expected_code);
        $ok_msg = (stripos($msg, $expected_table_fragment) !== false);

        if ($ok_code && $ok_msg) {
            echo "[PASS] test_table_not_found: code=$code msg=\"$msg\"\n";
            return true;
        } else {
            echo "[FAIL] test_table_not_found: got code=$code msg=\"$msg\" expected code=$expected_code and message containing '$expected_table_fragment'\n";
            return false;
        }
    } catch (Exception $e) {
        echo "[ERROR] test_table_not_found: unexpected exception: " . $e->getMessage() . "\n";
        return false;
    }
}

?>