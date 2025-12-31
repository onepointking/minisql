<?php
$start = microtime(true);
$mysqli = new mysqli("127.0.0.1", "root", "password", "", 3306);
$end = microtime(true);
if ($mysqli->connect_error) {
    echo "Connect error: " . $mysqli->connect_error . "\n";
} else {
    echo "Connected in " . (($end - $start) * 1000) . " ms\n";
    $mysqli->close();
}