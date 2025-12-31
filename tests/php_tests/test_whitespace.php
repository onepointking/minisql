<?php
/**
 * Test with various whitespace characters
 */

$host = '127.0.0.1';
$port = 3306;
$user = 'root';
$pass = 'password';
$db = 'test';

echo "Testing whitespace variations\n\n";

try {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    
    $mysqli = new mysqli($host, $user, $pass, $db, $port);
    
    // Test with different whitespace
    $queries = [
        "SELECT * FROM `layers` WHERE `name` = 'aerial'",  // Normal spaces
        "SELECT *\nFROM `layers` WHERE `name` = 'aerial'",  // Newline before FROM
        "SELECT *\tFROM `layers` WHERE `name` = 'aerial'",  // Tab before FROM  
        "SELECT * FROM`layers` WHERE `name` = 'aerial'",   // No space after FROM
        "SELECT *FROM `layers` WHERE `name` = 'aerial'",   // No space before FROM
    ];
    
    foreach ($queries as $i => $query) {
        echo "Test " . ($i+1) . ": " . json_encode($query) . "\n";
        try {
            $result = $mysqli->query($query);
            if ($result) {
                echo "Fields: ";
                $fields = [];
                while ($field = $result->fetch_field()) {
                    $fields[] = $field->name;
                }
                echo implode(', ', $fields) . "\n";
                
                $result->data_seek(0);
                $row = $result->fetch_assoc();
                echo "Result: ";
                print_r($row);
            }
        } catch (mysqli_sql_exception $e) {
            echo "Error: " . $e->getMessage() . "\n";
        }
        echo "\n";
    }
    
    $mysqli->close();
} catch (mysqli_sql_exception $e) {
    echo "Connection error: " . $e->getMessage() . "\n";
}
?>
