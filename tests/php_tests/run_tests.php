<?php
// run_tests.php
// Simple runner for the PHP MiniSQL error tests in this folder.

require_once __DIR__ . '/test_table_not_found.php';
require_once __DIR__ . '/test_column_not_found.php';
require_once __DIR__ . '/test_last_insert_id.php';
require_once __DIR__ . '/test_ok_packet_details.php';
require_once __DIR__ . '/test_simple_select.php';
require_once __DIR__ . '/test_select_star.php';
require_once __DIR__ . '/test_prepared_statements.php';
require_once __DIR__ . '/test_prepared_binary.php';
require_once __DIR__ . '/test_query_formats.php';
require_once __DIR__ . '/test_multiple_wheres_whereins.php';

// Load connection config from environment variables with sensible defaults
$cfg = [
    'host' => getenv('MINISQL_HOST') ?: '127.0.0.1',
    'port' => getenv('MINISQL_PORT') ?: '3306',
    'user' => getenv('MINISQL_USER') ?: 'root',
    'pass' => getenv('MINISQL_PASS') ?: 'password',
    'db'   => getenv('MINISQL_DB')   ?: 'minisql',
];

echo "Using connection: {$cfg['user']}@{$cfg['host']}:{$cfg['port']} db={$cfg['db']}\n";

$results = [];
$results[] = ['name' => 'test_table_not_found', 'ok' => test_table_not_found($cfg)];
$results[] = ['name' => 'test_column_not_found', 'ok' => test_column_not_found($cfg)];
$results[] = ['name' => 'test_last_insert_id', 'ok' => test_last_insert_id($cfg)];
$results[] = ['name' => 'test_ok_packet_details', 'ok' => test_ok_packet_details($cfg)];
$results[] = ['name' => 'test_simple_select', 'ok' => test_simple_select($cfg)];
$results[] = ['name' => 'test_select_star', 'ok' => test_select_star($cfg)];
$results[] = ['name' => 'test_prepared_statements', 'ok' => test_prepared_statements($cfg)];
$results[] = ['name' => 'test_prepared_binary', 'ok' => test_prepared_binary($cfg)];
$results[] = ['name' => 'test_query_formats', 'ok' => test_query_formats($cfg)];
$results[] = ['name' => 'test_multiple_wheres_whereins', 'ok' => test_multiple_wheres_whereins($cfg)];

$all_ok = true;
foreach ($results as $r) {
    if (!$r['ok']) $all_ok = false;
}

if ($all_ok) {
    echo "\nALL TESTS PASSED\n";
    exit(0);
} else {
    echo "\nSOME TESTS FAILED\n";
    exit(2);
}

?>