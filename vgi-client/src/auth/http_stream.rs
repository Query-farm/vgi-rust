// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Stream adapters shared by the plain and authenticated HTTP transports.

use arrow_array::RecordBatch;
use vgi_rpc::errors::Result;
use vgi_rpc::wire::Metadata;

use crate::transport::{ExchangeStream, ProducerStream};

pub(crate) struct HttpProducerAdapter<'c>(pub vgi_rpc_client::HttpStreamSession<'c>);

impl ProducerStream for HttpProducerAdapter<'_> {
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

pub(crate) struct HttpExchangeAdapter<'c>(pub vgi_rpc_client::HttpStreamSession<'c>);

impl ExchangeStream for HttpExchangeAdapter<'_> {
    fn header(&self) -> Option<&RecordBatch> {
        self.0.header().map(|(b, _md)| b)
    }
    fn exchange(&mut self, input: &RecordBatch) -> Result<Option<(RecordBatch, Metadata)>> {
        self.0.exchange(input, None)
    }
    fn exchange_with_metadata(
        &mut self,
        input: &RecordBatch,
        metadata: Option<&Metadata>,
    ) -> Result<Option<(RecordBatch, Metadata)>> {
        self.0.exchange(input, metadata)
    }
    fn close(&mut self) -> Result<()> {
        self.0.close()
    }
    fn cancel(&mut self) -> Result<()> {
        self.0.cancel()
    }
}
