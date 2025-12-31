<?php
// Test insertBatch() and updateBatch() behavior for CodeIgniter Model

// Ensure the bootstrap finds the CodeIgniter app inside `codeigniter-root`
defined('HOMEPATH') || define('HOMEPATH', realpath(__DIR__ . '/codeigniter-root') . DIRECTORY_SEPARATOR);

// Load CodeIgniter test bootstrap to initialize paths, constants and helpers
require_once HOMEPATH . 'vendor/codeigniter4/framework/system/Test/bootstrap.php';

use CodeIgniter\Model;

echo "CodeIgniter insertBatch/updateBatch test\n";
echo str_repeat('=', 40) . "\n\n";

try {
    // Minimal model that targets the 'tests' DB group (SQLite in-memory)
    class BatchModel extends Model
    {
        protected $table = 'items';
        protected $primaryKey = 'id';
        protected $DBGroup = 'tests';
        protected $allowedFields = ['id', 'name', 'qty'];
    }

    $model = new BatchModel();
    $db = $model->db;

    // Create table in the in-memory tests DB
    $db->query("DROP TABLE IF EXISTS items");
    $db->query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)");

    $data = [
        ['id' => 1, 'name' => 'alpha', 'qty' => 5],
        ['id' => 2, 'name' => 'beta',  'qty' => 7],
        ['id' => 3, 'name' => 'gamma', 'qty' => 9],
    ];

    echo "1) Test: insertBatch() returns SQL in testMode\n";
    $sqlInsert = $model->insertBatch($data, null, 100, true);
    if (is_array($sqlInsert)) {
        // batchExecute may return an array of SQL statements
        $sqlStr = implode("\n", $sqlInsert);
    } else {
        $sqlStr = (string) $sqlInsert;
    }

    echo "Generated SQL (truncated):\n";
    echo substr($sqlStr, 0, 800) . "\n\n";

    // Count rows encoded in VALUES by counting '),(' occurrences
    $valuesSets = substr_count($sqlStr, '),(') + 1;
    if ($valuesSets === count($data)) {
        echo "  ✓ insertBatch(testMode) contains {$valuesSets} value-sets (expected " . count($data) . ")\n";
    } else {
        echo "  ✗ insertBatch(testMode) contains {$valuesSets} value-sets (expected " . count($data) . ")\n";
    }

    echo "\n2) Test: insertBatch() actually inserts rows when executed\n";
    $res = $model->insertBatch($data);
    // For real execution, insertBatch returns number of rows or true-ish; check DB
    $row = $db->query("SELECT COUNT(*) as c FROM items")->getRow();
    $count = intval($row->c ?? 0);
    if ($count === count($data)) {
        echo "  ✓ Inserted {$count} rows\n";
    } else {
        echo "  ✗ DB reports {$count} rows (expected " . count($data) . ")\n";
    }

    echo "\n3) Test: updateBatch() returns SQL in testMode\n";
    $updated = [
        ['id' => 1, 'name' => 'alpha2', 'qty' => 6],
        ['id' => 2, 'name' => 'beta2',  'qty' => 8],
        ['id' => 3, 'name' => 'gamma2', 'qty' => 10],
    ];

    $sqlUpdate = $model->updateBatch($updated, 'id', 100, true);
    if (is_array($sqlUpdate)) {
        $sqlUpdateStr = implode("\n", $sqlUpdate);
    } else {
        $sqlUpdateStr = (string) $sqlUpdate;
    }

    echo "Generated UPDATE SQL (truncated):\n";
    echo substr($sqlUpdateStr, 0, 800) . "\n\n";

    $unionAllCount = substr_count($sqlUpdateStr, 'UNION ALL');
    $updateRows = $unionAllCount + 1;
    if ($updateRows === count($updated)) {
        echo "  ✓ updateBatch(testMode) contains {$updateRows} rows in SELECT/UNION (expected " . count($updated) . ")\n";
    } else {
        echo "  ✗ updateBatch(testMode) contains {$updateRows} rows (expected " . count($updated) . ")\n";
    }

    echo "\n4) Test: updateBatch() actually updates rows when executed\n";
    $model->updateBatch($updated, 'id');
    $results = $db->query("SELECT id, name, qty FROM items ORDER BY id")->getResult();
    $ok = true;
    foreach ($results as $i => $r) {
        $expect = $updated[$i];
        if ($r->name !== $expect['name'] || intval($r->qty) !== $expect['qty']) {
            $ok = false;
            echo "  ✗ Row id={$r->id} expected name={$expect['name']} qty={$expect['qty']} but got name={$r->name} qty={$r->qty}\n";
        }
    }
    if ($ok) {
        echo "  ✓ updateBatch applied correctly to all rows\n";
    }

    // Cleanup
    $db->query("DROP TABLE items");

    echo "\nAll tests complete.\n";

} catch (Throwable $e) {
    echo "Error: " . $e->getMessage() . "\n";
    echo $e->getTraceAsString();
    exit(1);
}

?>
