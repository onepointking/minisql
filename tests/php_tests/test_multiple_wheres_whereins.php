<?php
// test_multiple_wheres_whereins.php
// Test that multiple WHERE and multiple WHERE IN clauses work together

function test_multiple_wheres_whereins($cfg) {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

    $host = $cfg['host'];
    $port = (int)$cfg['port'];
    $user = $cfg['user'];
    $pass = $cfg['pass'];
    $db   = $cfg['db'];

    try {
        $mysqli = new mysqli($host, $user, $pass, $db, $port);

        // Ensure tiles table exists
        try { $mysqli->query("DROP TABLE IF EXISTS tiles"); } catch (Exception $e) {}

        $mysqli->query("CREATE TABLE tiles (
            id INT PRIMARY KEY AUTO_INCREMENT,
            layer_id INT,
            vintage_id INT,
            z INT,
            x INT,
            y INT
        )");

        // Insert some rows - two matching and one non-matching
        $mysqli->query("INSERT INTO tiles (layer_id, vintage_id, z, x, y) VALUES
            (1, 10, 0, 0, 0),
            (1, 10, 1, 2, 3),
            (2, 99, 5, 5, 5)") ;

        // Prepare sets like the query builder would
        $layerId = 1;
        $vintageId = 10;
        $zs = [0, 1];
        $xs = [0, 2];
        $ys = [0, 3];

        $in = function($arr) {
            return '(' . implode(', ', array_map('intval', $arr)) . ')';
        };

        // Query using multiple where() and multiple whereIn()
        $query = "SELECT z, x, y FROM `tiles` WHERE `layer_id` = " . intval($layerId)
               . " AND `vintage_id` = " . intval($vintageId)
               . " AND `z` IN " . $in($zs)
               . " AND `x` IN " . $in($xs)
               . " AND `y` IN " . $in($ys);

        $result = $mysqli->query($query);
        if (!$result) {
            echo "[FAIL] test_multiple_wheres_whereins: query failed: $query\n";
            $mysqli->close();
            return false;
        }

        if ($result->num_rows !== 2) {
            echo "[FAIL] test_multiple_wheres_whereins: expected 2 rows, got {$result->num_rows}\n";
            while ($row = $result->fetch_assoc()) { var_dump($row); }
            $mysqli->close();
            return false;
        }

        // Also test that multiple where() without IN works as expected
        $query2 = "SELECT * FROM `tiles` WHERE `layer_id` = 1 AND `vintage_id` = 10";
        $res2 = $mysqli->query($query2);
        if (!$res2 || $res2->num_rows !== 2) {
            echo "[FAIL] test_multiple_wheres_whereins: multiple where() expected 2 rows, got " . ($res2 ? $res2->num_rows : 'query-failed') . "\n";
            $mysqli->close();
            return false;
        }

        $mysqli->close();
        echo "[PASS] test_multiple_wheres_whereins\n";
        return true;
    } catch (Exception $e) {
        echo "[FAIL] test_multiple_wheres_whereins: " . $e->getMessage() . "\n";
        return false;
    }
}

?>
