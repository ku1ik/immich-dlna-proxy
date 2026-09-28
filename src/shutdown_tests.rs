use std::{
    net::{Ipv4Addr, TcpListener, TcpStream},
    process::{Child, Command},
    time::{Duration, Instant},
};

use super::*;

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// Signals must target a separate process, not the parallel test runner.
#[test]
fn normal_signals_exit_successfully() {
    const ADDRESS: &str = "IMMICH_DLNA_SHUTDOWN_TEST_ADDRESS";

    if let Ok(address) = std::env::var(ADDRESS) {
        // SAFETY: a valid, terminated interface name is passed to a read-only call.
        let interface_index = unsafe { libc::if_nametoindex(c"lo".as_ptr()) };
        assert_ne!(interface_index, 0);

        let config = Config {
            api_base: "http://127.0.0.1:9/api/".parse().unwrap(),
            api_key: http::HeaderValue::from_static("test-key"),
            listen_address: address.parse().unwrap(),
            friendly_name: "Shutdown test".into(),
            collator: config::collator("en").unwrap(),
            server_uuid: uuid::Uuid::new_v4(),
            log_level: tracing::Level::INFO,
            interface_index,
        };

        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(run(config))
            .unwrap();

        return;
    }

    for signal in [libc::SIGTERM, libc::SIGINT] {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let mut process = Process(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "shutdown_tests::normal_signals_exit_successfully",
                    "--nocapture",
                ])
                .env(ADDRESS, address.to_string())
                .spawn()
                .unwrap(),
        );

        let deadline = Instant::now() + Duration::from_secs(10);

        loop {
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "service exited before startup"
            );

            if TcpStream::connect(address).is_ok() {
                break;
            }

            assert!(Instant::now() < deadline, "service did not start");
            std::thread::sleep(Duration::from_millis(10));
        }

        // SAFETY: the child is still owned and unreaped; only it receives the signal.
        assert_eq!(
            unsafe { libc::kill(process.0.id() as libc::pid_t, signal) },
            0
        );

        loop {
            if let Some(status) = process.0.try_wait().unwrap() {
                assert!(status.success(), "signal {signal}: {status}");
                break;
            }

            assert!(Instant::now() < deadline, "service did not stop");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
