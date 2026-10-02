use std::{
    io::{self, ErrorKind},
    net::{SocketAddr, TcpListener as StdTcpListener},
    thread,
};

use tokio::{
    net::TcpListener,
    runtime::Builder as TokioRuntimeBuilder,
    select,
    time::{Duration, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[derive(Debug)]
pub struct TcpAcceptor {
    local_addr: SocketAddr,
    shutdown: CancellationToken,
}

impl Drop for TcpAcceptor {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl TcpAcceptor {
    pub fn spawn(listener: StdTcpListener) -> io::Result<TcpAcceptor> {
        let local_addr = listener.local_addr()?;
        let runtime = TokioRuntimeBuilder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()?;
        let tokio_listener = {
            let _guard = runtime.enter();
            TcpListener::from_std(listener)?
        };
        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();
        let builder = thread::Builder::new().name("rtc-tcp-acceptor".into());
        builder.spawn(move || {
            runtime.block_on(accept_loop(tokio_listener, local_addr, shutdown_clone));
        })?;
        Ok(Self {
            local_addr,
            shutdown,
        })
    }
}

const TCP_ACCEPT_BACKOFF_MAX: Duration = Duration::from_millis(100);

async fn accept_loop(listener: TcpListener, local_addr: SocketAddr, shutdown: CancellationToken) {
    let mut failure_backoff = Duration::from_millis(1);
    loop {
        select! {
            () = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() =>  {
                match accepted {
                    Ok((stream, peer)) => {
                        failure_backoff = Duration::from_millis(1);
                        drop(stream); // Placeholder
                    }
                    Err(error) => {
                        // Single client failures, shouldn't put the accept loop to sleep.
                        if matches!(
                            error.kind(),
                            ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset
                        ) {
                            continue;
                        }
                        warn!(%local_addr, ?error, "failed to accept rtc TCP connection");
                        sleep(failure_backoff).await;
                        failure_backoff = (failure_backoff * 2).min(TCP_ACCEPT_BACKOFF_MAX);
                    }
                }
            }
        }
    }
}
