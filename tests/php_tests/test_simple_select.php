<?php
// test_simple_select.php
// Test that simple SELECT without FROM still works

function test_simple_select($cfg) {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

    $host = $cfg['host'];
    $port = (int)$cfg['port'];
    $user = $cfg['user'];
    $pass = $cfg['pass'];
    $db   = $cfg['db'];

    try {
        $mysqli = new mysqli($host, $user, $pass, $db, $port);

        $tests = [
            "SELECT 1" => "1",
            "SELECT 1 AS test" => "test",
            "SELECT 42, 'hello' AS msg" => ["42", "msg"],
            "SELECT NULL" => "NULL",
            "SELECT 1+1" => "1+1",
        ];

        foreach ($tests as $query => $expected) {
            $result = $mysqli->query($query);
            if (!$result) {
                echo "[FAIL] test_simple_select: query failed: $query\n";
                $mysqli->close();
                return false;
            }
        }

        $mysqli->close();
        echo "[PASS] test_simple_select\n";
        return true;
    } catch (Exception $e) {
        echo "[FAIL] test_simple_select: " . $e->getMessage() . "\n";
        return false;
    }
}

?>
