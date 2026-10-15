use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};

use super::{ClientInfo, DefaultServerParameterProvider, StartupHandler};
use crate::api::PgWireConnectionState;
use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::{ReadyForQuery, TransactionStatus};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

#[async_trait]
pub trait NoopStartupHandler: StartupHandler {
    async fn post_startup<C>(
        &self,
        _client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }

    /// The ParameterStatus values startup announces (vendored change 4; see VENDORED.md).
    fn server_parameters(&self) -> DefaultServerParameterProvider {
        DefaultServerParameterProvider::default()
    }

    /// The session's process id and secret key, sent in BackendKeyData for a CancelRequest to
    /// name it (vendored change 7; see VENDORED.md). None keeps the client's default, 0 and 0.
    fn backend_key(&self) -> Option<(i32, crate::messages::startup::SecretKey)> {
        None
    }
}

#[async_trait]
impl<H> StartupHandler for H
where
    H: NoopStartupHandler,
{
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(ref startup) = message {
            super::protocol_negotiation(client, startup).await?;
            super::save_startup_parameters_to_metadata(client, startup);
            if let Some((pid, secret_key)) = self.backend_key() {
                client.set_pid_and_secret_key(pid, secret_key);
            }
            super::finish_authentication0(client, &self.server_parameters()).await?;

            self.post_startup(client, message).await?;

            client
                .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    TransactionStatus::Idle,
                )))
                .await?;
            client.set_state(PgWireConnectionState::ReadyForQuery);
        }

        Ok(())
    }
}

impl NoopStartupHandler for crate::api::NoopHandler {}
