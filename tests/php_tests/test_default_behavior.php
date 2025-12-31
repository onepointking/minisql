<?php
/**
 * Test to check what happens WITHOUT setting MYSQLI_OPT_INT_AND_FLOAT_NATIVE
 * This simulates the default CodeIgniter behavior
 */

$host = '127.0.0.1';
$port = 3306;
$user = 'root';
$pass = 'password';
$db = 'test';

echo "Testing WITHOUT MYSQLI_OPT_INT_AND_FLOAT_NATIVE\n";
echo "===============================================\n\n";

echo "PHP Version: " . phpversion() . "\n";
echo "MySQLi client info: " . mysqli_get_client_info() . "\n";
echo "Using mysqlnd: " . (strpos(mysqli_get_client_info(), 'mysqlnd') !== false ? 'YES' : 'NO') . "\n\n";

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    
    // Connect WITHOUT setting the option (like default CodeIgniter)
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    echo "Connected successfully!\n\n";
    
    // Create test table
    try {
        $mysqli->query("DROP TABLE IF EXISTS test_default");
    } catch (Exception $e) {}
    
    $mysqli->query("CREATE TABLE test_default (
        id INTEGER,
        count INTEGER,
        price FLOAT,
        active BOOLEAN,
        name VARCHAR(50)
    )");
    
    $mysqli->query("INSERT INTO test_default VALUES (1, 100, 19.99, true, 'Test')");
    $mysqli->query("INSERT INTO test_default VALUES (2, 0, 0.0, false, 'Zero')");
    
    echo "Query results:\n";
    $result = $mysqli->query("SELECT * FROM test_default");
    
    while ($row = $result->fetch_assoc()) {
        echo "\nRow:\n";
        foreach ($row as $key => $value) {
            $type = gettype($value);
            echo "  $key = ";
            var_export($value);
            echo " (type: $type)\n";
            
            // Test the 0 == false issue
            if ($key === 'count' || $key === 'active') {
                $is_falsy = !$value ? 'truthy/falsy: FALSY' : 'truthy/falsy: TRUTHY';
                $equals_false = ($value == false) ? 'equals false: YES' : 'equals false: NO';
                $equals_true = ($value == true) ? 'equals true: YES' : 'equals true: NO';
                echo "    -> $is_falsy, $equals_false, $equals_true\n";
            }
        }
    }
    
    echo "\n\nProblematic cases:\n";
    echo "------------------\n";
    $result = $mysqli->query("SELECT count, active FROM test_default WHERE id = 2");
    $row = $result->fetch_assoc();
    
    echo "Row where count=0 and active=false:\n";
    echo "  count value: ";
    var_export($row['count']);
    echo " (type: " . gettype($row['count']) . ")\n";
    echo "  active value: ";
    var_export($row['active']);
    echo " (type: " . gettype($row['active']) . ")\n";
    
    echo "\nThe Problem:\n";
    echo "  In PHP: 0 (int) == false is TRUE (correct)\n";
    echo "  In PHP: \"0\" (string) == false is TRUE (correct, but misleading)\n";
    echo "  In PHP: \"0\" (string) evaluates to FALSE in boolean context\n";
    echo "  BUT: if (\$row['count']) will be FALSE for both integer 0 and string \"0\"\n";
    echo "  AND: (\$row['count'] === 0) will FAIL if it's a string \"0\"\n";
    
    echo "\nActual behavior:\n";
    echo "  \$row['count'] == 0: " . ($row['count'] == 0 ? 'TRUE' : 'FALSE') . "\n";
    echo "  \$row['count'] === 0: " . ($row['count'] === 0 ? 'TRUE' : 'FALSE') . "\n";
    echo "  \$row['active'] == false: " . ($row['active'] == false ? 'TRUE' : 'FALSE') . "\n";
    echo "  \$row['active'] === false: " . ($row['active'] === false ? 'TRUE' : 'FALSE') . "\n";
    
    // Cleanup
    $mysqli->query("DROP TABLE test_default");
    $mysqli->close();
    
} catch (mysqli_sql_exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}
