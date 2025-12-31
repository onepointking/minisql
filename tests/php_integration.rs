use std::process::Stdio;
use std::time::Duration;

use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::process::Command;

use minisql::{Config, Server};

#[tokio::test]
async fn php_integration_tests() -> Result<(), Box<std::io::Error>> {
    // Check if PHP CLI is available; skip test if not
    match std::process::Command::new("php").arg("-v").stdout(Stdio::null()).status() {
        Ok(s) if s.success() => {}
        _ => {
            eprintln!("php CLI not available; skipping PHP integration tests");
            return Ok(());
        }
    }

    // Create a temporary data directory which will be removed when dropped
    let tmp = TempDir::new().expect("failed to create tempdir");

    // Find a free port by binding to port 0
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind to port 0");
    let addr = listener.local_addr().expect("failed to get local addr");
    let port = addr.port();
    drop(listener); // allow server to bind to the same port

    let mut cfg = Config::default();
    cfg.bind_addr = format!("127.0.0.1:{}", port).parse().expect("failed to parse bind addr");
    cfg.data_dir = tmp.path().to_path_buf();

    // Start server
    let server = Server::new(cfg.clone()).await.expect("failed to create server");
    let srv_handle = tokio::spawn(async move {
        // run will loop until aborted
        let _ = server.run().await;
    });

    // Wait for server to accept connections (try for a few seconds)
    let mut ready = false;
    for _ in 0..50 {
        match TcpStream::connect(("127.0.0.1", port)).await {
            Ok(_) => { ready = true; break; }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    if !ready {
        srv_handle.abort();
        panic!("Server did not become ready in time");
    }

    // Run the PHP test harness
    let mut cmd = Command::new("php");
    cmd.arg("tests/php_tests/run_tests.php")
        .env("MINISQL_HOST", "127.0.0.1")
        .env("MINISQL_PORT", port.to_string())
        .env("MINISQL_USER", cfg.username.clone())
        .env("MINISQL_PASS", cfg.password.clone())
        // many PHP tests expect the 'test' database
        .env("MINISQL_DB", "test")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let php_future = cmd.output();

    // Timeout the PHP tests in case something hangs
    let output = match tokio::time::timeout(Duration::from_secs(30), php_future).await {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => {
            srv_handle.abort();
            return Err(Box::new(e));
        }
        Err(_) => {
            srv_handle.abort();
            panic!("PHP tests timed out");
        }
    };

    // Print output for debugging on failure
    if !output.status.success() {
        eprintln!("PHP tests failed. stdout:\n{}", String::from_utf8_lossy(&output.stdout));
        eprintln!("PHP tests failed. stderr:\n{}", String::from_utf8_lossy(&output.stderr));
        srv_handle.abort();
        panic!("PHP tests returned non-zero status: {:?}", output.status.code());
    }

    // Clean up: stop server
    srv_handle.abort();

    Ok(())
}
