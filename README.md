# MiniSQL

A MySQL-compatible SQL server written in Rust, featuring dual storage engines, ACID transactions, JSON support, and CodeIgniter/Laravel compatibility.

## Features

- **MySQL Protocol Compatible** - Works with mysql CLI, PHP MySQLi, PDO, and ORMs like CodeIgniter/Laravel
- **Dual Storage Engines** - Choose between Granite (durable, transactional) or Sandstone (fast, CRDT-based)
- **ACID Transactions** - Write-ahead logging, crash recovery, Read Committed isolation
- **JSON Support** - Native JSON columns with `->` and `->>` operators
- **Prepared Statements** - Binary protocol with native type support

## Quick Start

```bash
# Build
cargo build --release

# Run with defaults (port 3306, user: root, password: password)
./target/release/minisql

# Or with custom settings
./target/release/minisql --port 3307 --data-dir ./mydata --user admin --password secret
```

### Connect with MySQL Client

```bash
mysql -h 127.0.0.1 -P 3306 -u root -ppassword
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `-p, --port` | Port to listen on | 3306 |
| `-d, --data-dir` | Data directory | ./data |
| `-u, --user` | Username | root |
| `-P, --password` | Password | password |

---

## Storage Engines

MiniSQL supports two storage engines:

### Granite (Default)

Durable, transactional storage with write-ahead logging.

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100)) ENGINE=Granite;
```

- ✅ ACID transactions with WAL
- ✅ Crash recovery
- ✅ Durable to disk
- ✅ Group-commit optimization for high throughput

### Sandstone

Fast, in-memory storage with CRDT-based conflict resolution.

```sql
CREATE TABLE cache (key VARCHAR(255) PRIMARY KEY, value TEXT) ENGINE=Sandstone;
```

- ✅ High-speed in-memory operations
- ✅ Last-writer-wins conflict resolution
- ✅ Suitable for caches and temporary data
- ⚠️ Data not persisted (memory only)
- ⚠️ Transactions silently ignored (like MySQL's MyISAM)

### Switching Engines

```sql
ALTER TABLE users ENGINE = Sandstone;
ALTER TABLE users ENGINE = Granite;
```

---

## Supported SQL

### Data Definition (DDL)

```sql
-- Tables
CREATE TABLE name (col1 TYPE, col2 TYPE, ...);
CREATE TABLE IF NOT EXISTS name (...);
DROP TABLE name;
DROP TABLE IF EXISTS name;
TRUNCATE TABLE name;

-- Indexes
CREATE INDEX idx_name ON table(column);
CREATE INDEX idx_name ON table(col1, col2);  -- Composite
DROP INDEX idx_name ON table;

-- Introspection
SHOW TABLES;
DESCRIBE table_name;

-- Maintenance
VACUUM;      -- Rebuild and compact database
CHECKPOINT;  -- Force WAL checkpoint
```

### Data Types

| Type | Description |
|------|-------------|
| `INTEGER` / `INT` / `BIGINT` | 64-bit signed integer |
| `FLOAT` / `DOUBLE` / `REAL` | 64-bit floating point |
| `VARCHAR(n)` | Variable-length string |
| `TEXT` | Unlimited string |
| `BOOLEAN` / `BOOL` | True/False |
| `JSON` | JSON document |

### Data Manipulation (DML)

```sql
-- Insert
INSERT INTO table (col1, col2) VALUES (val1, val2);
INSERT INTO table VALUES (v1, v2), (v3, v4);

-- Select
SELECT * FROM table;
SELECT col1, col2 FROM table WHERE condition;
SELECT * FROM table ORDER BY col ASC/DESC;
SELECT * FROM table LIMIT 10 OFFSET 5;
SELECT table.* FROM table;  -- Qualified star

-- Update
UPDATE table SET col = value WHERE condition;

-- Delete
DELETE FROM table WHERE condition;
```

### WHERE Operators

```sql
-- Comparison
=, <>, !=, <, <=, >, >=

-- Logical
AND, OR, NOT

-- Null checks
IS NULL, IS NOT NULL

-- Pattern matching
LIKE 'pattern%'

-- Set membership
IN (val1, val2, val3)
NOT IN (val1, val2)
```

### Aggregate Functions

```sql
SELECT COUNT(*) FROM table;
SELECT SUM(col), AVG(col), MIN(col), MAX(col) FROM table;
SELECT category, COUNT(*) FROM products GROUP BY category;
```

### JOINs

```sql
SELECT * FROM a INNER JOIN b ON a.id = b.a_id;
SELECT * FROM a LEFT JOIN b ON a.id = b.a_id;
SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id;

-- Multiple joins
SELECT u.*, o.*, p.*
FROM users u
JOIN orders o ON u.id = o.user_id
JOIN products p ON o.product_id = p.id;
```

### Transactions

```sql
BEGIN;
-- ... operations ...
COMMIT;

-- Or
BEGIN;
-- ... operations ...
ROLLBACK;
```

---

## JSON Support

```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    data JSON
);

INSERT INTO products VALUES (1, '{"name": "Widget", "price": 9.99, "tags": ["sale"]}');

-- Extract as JSON (quoted)
SELECT data->'name' FROM products;  -- Returns: "Widget"

-- Extract as text (unquoted)  
SELECT data->>'name' FROM products;  -- Returns: Widget

-- Filter by JSON field
SELECT * FROM products WHERE data->>'price' > '5';
```

---

## PHP/CodeIgniter Compatibility

MiniSQL implements the MySQL binary protocol correctly, ensuring native PHP types:

```php
// Prepared statements return native types automatically
$stmt = $mysqli->prepare("SELECT id, price FROM products WHERE id = ?");
$stmt->bind_param("i", $id);
$stmt->execute();
$result = $stmt->get_result();
$row = $result->fetch_assoc();

// $row['id'] is integer, not string
// $row['price'] is float, not string
```

Works with:
- ✅ PHP MySQLi (prepared statements)
- ✅ PHP PDO
- ✅ CodeIgniter Query Builder
- ✅ Laravel Eloquent

---

## Architecture

```
┌─────────────────────────────────────────────┐
│              MySQL Clients                   │
└────────────────────┬────────────────────────┘
                     ▼
┌─────────────────────────────────────────────┐
│            Protocol Handler                  │
│    (MySQL wire protocol, authentication)     │
└────────────────────┬────────────────────────┘
                     ▼
┌─────────────────────────────────────────────┐
│              SQL Parser                      │
│       (Lexer → Parser → AST)                 │
└────────────────────┬────────────────────────┘
                     ▼
┌─────────────────────────────────────────────┐
│            Query Executor                    │
└───────────┬─────────────────┬───────────────┘
            ▼                 ▼
┌───────────────────┐ ┌───────────────────────┐
│  Granite Engine   │ │   Sandstone Engine    │
│  (WAL, durable)   │ │   (CRDT, in-memory)   │
└───────────────────┘ └───────────────────────┘
            ▼
┌─────────────────────────────────────────────┐
│              File System                     │
│     (wal.log, catalog.json, tables/*.dat)   │
└─────────────────────────────────────────────┘
```

---

## Configuration

### Environment Variables

```bash
RUST_LOG=debug ./target/release/minisql  # Enable debug logging
```

### Data Directory Layout

```
data/
├── catalog.json      # Table schemas and metadata
├── wal.log           # Write-ahead log (Granite)
├── wal.checkpoint    # Checkpoint marker
└── tables/
    └── *.dat         # Table data files
```

---

## Limitations

This is designed as an educational/lightweight SQL server. Notable limitations:

### Not Implemented
- Subqueries
- Views
- Stored procedures / triggers
- User management (single configured user)
- Multiple databases
- Foreign key constraints

### Performance Considerations
- All data loaded into memory
- Table-level locking (not row-level)
- No query optimizer / query planner
- Full table scans (indexes help with lookups)

### Isolation
- Read Committed only (no Serializable)
- No deadlock detection

---

## Testing

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test --test in_operator_tests
cargo test --test sandstone_tests
cargo test --test vacuum_tests

# With release optimizations
cargo test --release

# PHP integration tests (requires running server)
cd tests/php_tests && php run_tests.php
```

---

## Utilities

### capture_mysql_packets.py

A MySQL packet capture proxy for debugging protocol issues:

```bash
# Proxy connections and log packets
python3 capture_mysql_packets.py --listen-port 3307 --server-port 3306
```

---

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.
