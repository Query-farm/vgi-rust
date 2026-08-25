// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The seam between the VGI protocol layer and whatever moves bytes.
//!
//! `vgi-rpc-client` ships two concrete clients — [`RpcClient`] for the
//! byte-stream transports (subprocess, AF_UNIX, TCP) and `HttpClient` — with
//! matching method signatures but no common trait, and stream session types
//! that differ. [`VgiTransport`] supplies one trait over both, boxing the
//! session behind [`ProducerStream`].
//!
//! Keeping the protocol layer generic over this trait rather than over a
//! concrete client is what lets an async driver be added later without touching
//! any catalog or scan code, and it mirrors how the C++ extension factors the
//! same concern: `PerformBindProtocol` takes a `BindTransportFn` callback and
//! knows nothing about subprocess versus HTTP.

use arrow_array::RecordBatch;
use vgi_rpc::errors::Result;
use vgi_rpc::wire::Metadata;
use vgi_rpc_client::RpcClient;

/// An open producer stream: the worker emits, the client ticks.
pub trait ProducerStream {
    /// The stream header batch, if the stream declared one.
    ///
    /// For `init` this carries the `GlobalInitResponse` — the worker-minted
    /// `execution_id` and its `max_workers` advice.
    fn header(&self) -> Option<&RecordBatch>;

    /// Pull the next batch. `None` means end of stream.
    ///
    /// Worker log batches are dispatched by the underlying client and never
    /// surface here.
    fn tick(&mut self) -> Result<Option<(RecordBatch, Metadata)>>;

    /// Pull the next batch while attaching application metadata to this tick.
    ///
    /// Older/custom transports that do not override this method retain the
    /// ordinary producer behaviour and ignore the metadata. That is a safe
    /// fallback for optimisation hints such as dynamic filters: the engine
    /// still evaluates the join or predicate locally.
    fn tick_with_metadata(
        &mut self,
        _metadata: Option<&Metadata>,
    ) -> Result<Option<(RecordBatch, Metadata)>> {
        self.tick()
    }

    /// Ask the worker to stop early.
    fn cancel(&mut self) -> Result<()>;
}

/// An open exchange stream: the client sends batches, the worker answers.
pub trait ExchangeStream {
    /// The stream header batch, if the stream declared one.
    fn header(&self) -> Option<&RecordBatch>;

    /// Send one input batch and read the worker's answer.
    ///
    /// A zero-row answer is a legitimate "nothing for that input", not end of
    /// stream — `None` is end of stream.
    fn exchange(&mut self, input: &RecordBatch) -> Result<Option<(RecordBatch, Metadata)>>;

    /// Signal input EOS and drain. Idempotent.
    fn close(&mut self) -> Result<()>;

    /// Ask the worker to stop early.
    fn cancel(&mut self) -> Result<()>;
}

/// Something that can carry the VGI protocol.
pub trait VgiTransport: Send {
    /// Send `params` as a one-row request batch and return the response batch.
    fn call_unary(&mut self, method: &str, params: &RecordBatch) -> Result<RecordBatch>;

    /// Open a producer stream for `method`.
    fn open_producer<'a>(
        &'a mut self,
        method: &str,
        params: &RecordBatch,
        metadata: Option<Metadata>,
        has_header: bool,
    ) -> Result<Box<dyn ProducerStream + 'a>>;

    /// Open an exchange stream for `method`.
    fn open_exchange<'a>(
        &'a mut self,
        method: &str,
        params: &RecordBatch,
        has_header: bool,
    ) -> Result<Box<dyn ExchangeStream + 'a>>;

    /// A short label for this connection, used in error messages.
    fn label(&self) -> &str;
}

/// A byte-stream transport: subprocess, AF_UNIX, or TCP.
pub struct StreamTransport {
    client: RpcClient,
    label: String,
}

impl StreamTransport {
    /// Wrap an already-connected [`RpcClient`].
    pub fn new(client: RpcClient, label: impl Into<String>) -> Self {
        Self {
            client,
            label: label.into(),
        }
    }
}

struct StreamSessionAdapter<'c>(vgi_rpc_client::StreamSession<'c>);

impl ProducerStream for StreamSessionAdapter<'_> {
    fn header(&self) -> Option<&RecordBatch> {
        self.0.header().map(|(b, _md)| b)
    }

    fn tick(&mut self) -> Result<Option<(RecordBatch, Metadata)>> {
        self.0.tick()
    }

    fn tick_with_metadata(
        &mut self,
        metadata: Option<&Metadata>,
    ) -> Result<Option<(RecordBatch, Metadata)>> {
        self.0.tick_with_metadata(metadata)
    }

    fn cancel(&mut self) -> Result<()> {
        self.0.cancel()
    }
}

struct StreamExchangeAdapter<'c>(vgi_rpc_client::StreamSession<'c>);

impl ExchangeStream for StreamExchangeAdapter<'_> {
    fn header(&self) -> Option<&RecordBatch> {
        self.0.header().map(|(b, _md)| b)
    }

    fn exchange(&mut self, input: &RecordBatch) -> Result<Option<(RecordBatch, Metadata)>> {
        self.0.exchange(input, None)
    }

    fn close(&mut self) -> Result<()> {
        self.0.close()
    }

    fn cancel(&mut self) -> Result<()> {
        self.0.cancel()
    }
}

impl VgiTransport for StreamTransport {
    fn call_unary(&mut self, method: &str, params: &RecordBatch) -> Result<RecordBatch> {
        let (batch, _md) = self.client.call_unary(method, params, None)?;
        Ok(batch)
    }

    fn open_producer<'a>(
        &'a mut self,
        method: &str,
        params: &RecordBatch,
        metadata: Option<Metadata>,
        has_header: bool,
    ) -> Result<Box<dyn ProducerStream + 'a>> {
        let session = self
            .client
            .open_producer(method, params, metadata.as_ref(), has_header)?;
        Ok(Box::new(StreamSessionAdapter(session)))
    }

    fn open_exchange<'a>(
        &'a mut self,
        method: &str,
        params: &RecordBatch,
        has_header: bool,
    ) -> Result<Box<dyn ExchangeStream + 'a>> {
        let session = self
            .client
            .open_exchange(method, params, None, has_header)?;
        Ok(Box::new(StreamExchangeAdapter(session)))
    }

    fn label(&self) -> &str {
        &self.label
    }
}

#[cfg(feature = "http")]
mod http_impl {
    use super::{ExchangeStream, Metadata, ProducerStream, RecordBatch, Result, VgiTransport};
    use vgi_rpc_client::HttpClient;

    /// An HTTP transport.
    pub struct HttpTransport {
        client: HttpClient,
        label: String,
    }

    impl HttpTransport {
        /// Wrap an already-connected `HttpClient`.
        pub fn new(client: HttpClient, label: impl Into<String>) -> Self {
            Self {
                client,
                label: label.into(),
            }
        }
    }

    struct HttpSessionAdapter<'c>(vgi_rpc_client::HttpStreamSession<'c>);

    impl ProducerStream for HttpSessionAdapter<'_> {
        fn header(&self) -> Option<&RecordBatch> {
            self.0.header().map(|(b, _md)| b)
        }

        fn tick(&mut self) -> Result<Option<(RecordBatch, Metadata)>> {
            self.0.tick()
        }

        fn tick_with_metadata(
            &mut self,
            metadata: Option<&Metadata>,
        ) -> Result<Option<(RecordBatch, Metadata)>> {
            self.0.tick_with_metadata(metadata)
        }

        fn cancel(&mut self) -> Result<()> {
            self.0.cancel()
        }
    }

    struct HttpExchangeAdapter<'c>(vgi_rpc_client::HttpStreamSession<'c>);

    impl ExchangeStream for HttpExchangeAdapter<'_> {
        fn header(&self) -> Option<&RecordBatch> {
            self.0.header().map(|(b, _md)| b)
        }

        fn exchange(&mut self, input: &RecordBatch) -> Result<Option<(RecordBatch, Metadata)>> {
            self.0.exchange(input, None)
        }

        fn close(&mut self) -> Result<()> {
            self.0.close()
        }

        fn cancel(&mut self) -> Result<()> {
            self.0.cancel()
        }
    }

    impl VgiTransport for HttpTransport {
        fn call_unary(&mut self, method: &str, params: &RecordBatch) -> Result<RecordBatch> {
            let (batch, _md) = self.client.call_unary(method, params, None)?;
            Ok(batch)
        }

        fn open_producer<'a>(
            &'a mut self,
            method: &str,
            params: &RecordBatch,
            metadata: Option<Metadata>,
            has_header: bool,
        ) -> Result<Box<dyn ProducerStream + 'a>> {
            let session =
                self.client
                    .open_producer(method, params, metadata.as_ref(), has_header)?;
            Ok(Box::new(HttpSessionAdapter(session)))
        }

        fn open_exchange<'a>(
            &'a mut self,
            method: &str,
            params: &RecordBatch,
            has_header: bool,
        ) -> Result<Box<dyn ExchangeStream + 'a>> {
            let session = self
                .client
                .open_exchange(method, params, None, has_header)?;
            Ok(Box::new(HttpExchangeAdapter(session)))
        }

        fn label(&self) -> &str {
            &self.label
        }
    }
}

#[cfg(feature = "http")]
pub use http_impl::HttpTransport;
