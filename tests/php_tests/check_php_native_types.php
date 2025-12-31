<?php
/**
 * Check PHP's actual behavior with native types
 */

echo "PHP Version: " . PHP_VERSION . "\n";
echo "PHP_VERSION_ID: " . PHP_VERSION_ID . "\n";
echo "\n";

// Check if MYSQLI_OPT_INT_AND_FLOAT_NATIVE is defined
if (defined('MYSQLI_OPT_INT_AND_FLOAT_NATIVE')) {
    echo "MYSQLI_OPT_INT_AND_FLOAT_NATIVE is defined (value: " . MYSQLI_OPT_INT_AND_FLOAT_NATIVE . ")\n";
} else {
    echo "MYSQLI_OPT_INT_AND_FLOAT_NATIVE is NOT defined\n";
}

echo "\n";
echo "According to PHP documentation:\n";
echo "- PHP 5.3.0: MYSQLI_OPT_INT_AND_FLOAT_NATIVE added, OFF by default\n";
echo "- PHP 8.1.0: Native types enabled by default for mysqlnd\n";
echo "\n";

if (PHP_VERSION_ID >= 80100) {
    echo "Your PHP version (>= 8.1.0) should have native types ENABLED by default\n";
    echo "BUT this may not work correctly with all servers\n";
} else {
    echo "Your PHP version (< 8.1.0) has native types DISABLED by default\n";
    echo "You need to explicitly set MYSQLI_OPT_INT_AND_FLOAT_NATIVE\n";
}

echo "\n";
echo "Note: Some sources indicate that the automatic conversion in PHP 8.1+ \n";
echo "might have bugs or might not be fully implemented yet.\n";
echo "The most reliable approach is to explicitly set MYSQLI_OPT_INT_AND_FLOAT_NATIVE\n";
?>
