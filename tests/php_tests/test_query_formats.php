<?php
// test_query_formats.php
// Ensure various query spacing/formatting options are accepted

function test_query_formats($cfg) {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

    $host = $cfg['host'];
    $port = (int)$cfg['port'];
    $user = $cfg['user'];
    $pass = $cfg['pass'];
    $db   = $cfg['db'];

    try {
        $mysqli = new mysqli($host, $user, $pass, $db, $port);

        // Ensure a table exists
        try { $mysqli->query("CREATE TABLE IF NOT EXISTS layers (name VARCHAR(100), id INT PRIMARY KEY)"); } catch (Exception $e) {}
        try { $mysqli->query("INSERT INTO layers (name, id) VALUES ('aerial', 1)"); } catch (Exception $e) {}

        $queries = [
            "SELECT * FROM `layers` WHERE `name` = 'aerial'",
            "SELECT*FROM`layers`WHERE`name`='aerial'",
            "SELECT * FROM `layers` WHERE `name`='aerial'",
            "SELECT   *   FROM   `layers`   WHERE   `name` = 'aerial'",
        ];

        foreach ($queries as $q) {
            try {
                $r = $mysqli->query($q);
                if (!$r) { echo "[FAIL] test_query_formats: query failed: $q\n"; return false; }
            } catch (Exception $e) {
                echo "[FAIL] test_query_formats: exception for query: $q - " . $e->getMessage() . "\n"; return false;
            }
        }

        echo "[PASS] test_query_formats\n";
        $mysqli->close();
        return true;
    } catch (Exception $e) {
        echo "[FAIL] test_query_formats: " . $e->getMessage() . "\n";
        return false;
    }
}

?>
