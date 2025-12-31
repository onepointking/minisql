<?php
// test_ok_packet_details.php
// Verify OK packet details (affected_rows / insert_id) on simple statements

function test_ok_packet_details($cfg) {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

    $host = $cfg['host'];
    $port = (int)$cfg['port'];
    $user = $cfg['user'];
    $pass = $cfg['pass'];
    $db   = $cfg['db'];

    $table = 'okpkt_'.uniqid();

    try {
        $mysqli = new mysqli($host, $user, $pass, $db, $port);
        try { $mysqli->query("DROP TABLE IF EXISTS $table"); } catch (Exception $e) {}
        $mysqli->query("CREATE TABLE $table (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)");
        $mysqli->query("INSERT INTO $table (name) VALUES ('a')");
        $id = $mysqli->insert_id;
        if (!$id) { echo "[FAIL] test_ok_packet_details: insert_id missing\n"; return false; }

        $res = $mysqli->query("UPDATE $table SET name = 'b' WHERE id = $id");
        if ($mysqli->affected_rows !== 1) { echo "[FAIL] test_ok_packet_details: affected_rows expected 1 got " . $mysqli->affected_rows . "\n"; return false; }

        $mysqli->query("DROP TABLE $table");
        $mysqli->close();
        echo "[PASS] test_ok_packet_details\n";
        return true;
    } catch (Exception $e) {
        echo "[FAIL] test_ok_packet_details: " . $e->getMessage() . "\n";
        return false;
    }
}

?>
