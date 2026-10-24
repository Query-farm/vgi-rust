// Copyright 2025, 2026 Query Farm LLC - https://query.farm

fn main() {
    let mut worker = vgi::Worker::new();
    worker.register_table(vgi_external_data_worker::ReadObservations);
    // This example deliberately serves native stdio only. Its live file
    // cursor is not serializable for stateless HTTP continuation.
    vgi::transport::serve_stdio(std::sync::Arc::new(worker.build_server()));
}
