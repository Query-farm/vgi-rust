// Copyright 2025, 2026 Query Farm LLC - https://query.farm

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_select::filter::filter_record_batch;
use vgi::vgi_rpc::errors::Code;
use vgi_client::testing::{scan_options, Comparison, FilterExpr, ScanResult, TestWorker};
use vgi_client::{Arguments, BindSpec, Result, RpcError, ScanOptions, WorkerLogMessage};
use vgi_external_data_worker::observations_schema;

fn worker() -> Result<TestWorker> {
    TestWorker::connect_subprocess(
        &[env!("CARGO_BIN_EXE_vgi-external-data-worker")],
        "observations",
    )
}

fn source(name: &str) -> BindSpec {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name);
    BindSpec::table("read_observations")
        .in_schema("main")
        .with_arguments(Arguments::new().positional(directory.to_str().unwrap()))
}

fn latitude(comparison: Comparison, value: f64) -> FilterExpr {
    FilterExpr::compare(
        "latitude",
        comparison,
        Arc::new(Float64Array::from(vec![value])),
    )
}

/// Reference selection deliberately uses actual rows and ordinary Rust
/// predicates, independently of VGI's filter evaluator and pruning API.
fn select(
    full: &ScanResult,
    predicate: impl Fn(&RecordBatch, usize) -> bool,
) -> Result<ScanResult> {
    let batches = full
        .batches()
        .iter()
        .map(|batch| {
            let mask = BooleanArray::from(
                (0..batch.num_rows())
                    .map(|row| predicate(batch, row))
                    .collect::<Vec<_>>(),
            );
            filter_record_batch(batch, &mask)
                .map_err(|error| RpcError::runtime_error(error.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    ScanResult::try_new(full.schema().clone(), batches)
}

fn latitudes(batch: &RecordBatch) -> &Float64Array {
    batch.column(0).as_any().downcast_ref().unwrap()
}

#[test]
fn round_trip_preserves_exact_types_and_values_across_batches() -> Result<()> {
    let actual = worker()?.scan(&source("observations"), &ScanOptions::default())?;
    assert!(
        actual.batches().len() > 1,
        "fixture must exercise streaming"
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(vec![
            40.8, 40.8, 40.8, 41.2, 41.2, 41.2, 42.0, 42.0, 42.0,
        ])),
        Arc::new(Int64Array::from(vec![
            9_007_199_254_740_993,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
        ])),
        Arc::new(UInt64Array::from(vec![
            9_223_372_036_854_775_808,
            u64::MAX,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
        ])),
        Arc::new(StringArray::from(vec![
            Some("south sensor"),
            None,
            Some("duplicate"),
            Some("central sensor"),
            None,
            Some("duplicate"),
            Some("north sensor"),
            None,
            Some("duplicate"),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(10.5),
            None,
            Some(3.5),
            Some(20.5),
            None,
            Some(6.5),
            Some(30.5),
            None,
            Some(9.5),
        ])),
    ];
    let expected = ScanResult::try_new(
        observations_schema(),
        vec![RecordBatch::try_new(observations_schema(), columns).unwrap()],
    )?;
    actual.assert_matches(&expected)
}

#[test]
fn floating_range_and_filter_only_column_survive_reordered_projection() -> Result<()> {
    // SELECT label, id WHERE latitude > 40.75 AND latitude < 41
    //                       AND reading IS NOT NULL
    let filter = latitude(Comparison::Gt, 40.75)
        .and(latitude(Comparison::Lt, 41.0))
        .and(FilterExpr::is_not_null("reading"));
    let options = scan_options(&observations_schema(), &filter, Some(vec![3, 1]))?;
    worker()?.check_pushdown(&source("observations"), &options, |full| {
        select(full, |batch, row| {
            latitudes(batch).value(row) > 40.75
                && latitudes(batch).value(row) < 41.0
                && !batch.column(4).is_null(row)
        })
    })
}

#[test]
fn unread_column_in_or_is_unknown_instead_of_null() -> Result<()> {
    // This is the NULL-substitution regression: central/north can still match.
    let filter = latitude(Comparison::Eq, 40.8).or(FilterExpr::is_not_null("reading"));
    let options = scan_options(&observations_schema(), &filter, None)?;
    worker()?.check_pushdown(&source("observations"), &options, |full| {
        let expected = select(full, |batch, row| {
            latitudes(batch).value(row) == 40.8 || !batch.column(4).is_null(row)
        })?;
        assert_eq!(expected.num_rows(), 7);
        Ok(expected)
    })
}

#[test]
fn nulls_and_duplicate_projected_values_are_counted() -> Result<()> {
    for nullable in [true, false] {
        let filter = if nullable {
            FilterExpr::is_null("label")
        } else {
            FilterExpr::is_not_null("reading")
        };
        let options = scan_options(&observations_schema(), &filter, Some(vec![3]))?;
        worker()?.check_pushdown(&source("observations"), &options, |full| {
            select(full, |batch, row| {
                if nullable {
                    batch.column(3).is_null(row)
                } else {
                    !batch.column(4).is_null(row)
                }
            })
        })?;
    }
    Ok(())
}

#[test]
fn integer_filter_literals_keep_their_signedness_and_precision() -> Result<()> {
    let filter = FilterExpr::compare(
        "id",
        Comparison::Eq,
        Arc::new(Int64Array::from(vec![9_007_199_254_740_993])),
    )
    .and(FilterExpr::compare(
        "count",
        Comparison::GtEq,
        Arc::new(UInt64Array::from(vec![9_223_372_036_854_775_808])),
    ));
    let options = scan_options(&observations_schema(), &filter, Some(vec![2, 1]))?;
    worker()?.check_pushdown(&source("observations"), &options, |full| {
        select(full, |batch, row| {
            let ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            ids.value(row) == 9_007_199_254_740_993
                && counts.value(row) >= 9_223_372_036_854_775_808
        })
    })
}

#[test]
fn typed_source_errors_never_become_fill_rows() -> Result<()> {
    for (fixture, code) in [
        ("does-not-exist", Code::NotFound),
        ("missing-partition", Code::NotFound),
        ("malformed", Code::DataLoss),
    ] {
        let error = worker()?
            .scan(&source(fixture), &ScanOptions::default())
            .unwrap_err();
        assert_eq!(error.code(), code, "{fixture}: {error}");
    }
    let empty_argument = BindSpec::table("read_observations")
        .in_schema("main")
        .with_arguments(Arguments::new().positional(""));
    let error = worker()?
        .scan(&empty_argument, &ScanOptions::default())
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    Ok(())
}

#[test]
fn excluded_partition_is_never_opened() -> Result<()> {
    // The northern file is absent. It is harmless only when pruning proves
    // that no row in that partition can contribute to this query.
    let options = scan_options(
        &observations_schema(),
        &latitude(Comparison::Lt, 41.0),
        None,
    )?;
    let actual = worker()?.scan(&source("prunable-missing"), &options)?;
    assert_eq!(actual.num_rows(), 1);
    let error = worker()?
        .scan(&source("prunable-missing"), &ScanOptions::default())
        .unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
    Ok(())
}

#[test]
fn pruning_every_partition_preserves_the_projected_empty_schema() -> Result<()> {
    let options = scan_options(
        &observations_schema(),
        &latitude(Comparison::Gt, 100.0),
        Some(vec![1, 3]),
    )?;
    worker()?.check_pushdown(&source("observations"), &options, |full| {
        select(full, |batch, row| latitudes(batch).value(row) > 100.0)
    })
}

#[test]
fn diagnostics_arrive_with_the_execution_and_source_counters() -> Result<()> {
    let logs = Arc::new(Mutex::new(Vec::<WorkerLogMessage>::new()));
    let captured = logs.clone();
    let mut worker = worker()?;
    assert!(worker
        .client_mut()
        .set_worker_log_sink(Arc::new(move |message| {
            captured.lock().unwrap().push(message);
        })));
    let filter = latitude(Comparison::Lt, 41.0).and(FilterExpr::is_not_null("reading"));
    let options = scan_options(&observations_schema(), &filter, Some(vec![3]))?;
    assert_eq!(
        worker.scan(&source("observations"), &options)?.num_rows(),
        2
    );

    let logs = logs.lock().unwrap();
    let extra = |message: &WorkerLogMessage, key: &str| {
        message
            .extras
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    };
    let started = logs
        .iter()
        .find(|message| extra(message, "event").as_deref() == Some("scan.started"))
        .unwrap();
    let finished = logs
        .iter()
        .find(|message| extra(message, "event").as_deref() == Some("scan.finished"))
        .unwrap();
    let execution_id = extra(started, "execution_id").unwrap();
    assert!(!execution_id.is_empty());
    assert_eq!(extra(finished, "execution_id").unwrap(), execution_id);
    assert_eq!(
        extra(finished, "function").as_deref(),
        Some("read_observations")
    );
    assert_eq!(extra(finished, "rows_read").as_deref(), Some("3"));
    assert_eq!(extra(finished, "chunks_read").as_deref(), Some("1"));
    assert_eq!(extra(finished, "chunks_skipped").as_deref(), Some("2"));
    assert!(
        extra(finished, "bytes_read")
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );
    Ok(())
}
