use super::*;
use arrow_array::{Decimal128Array, Float32Array, Float64Array, Int64Array, UInt64Array};

fn column(index: usize, name: &str, data_type: DataType) -> Expr {
    Expr::Column {
        index,
        name: name.into(),
        data_type,
    }
}

fn cmp(op: &str, left: Expr, right: Expr) -> Expr {
    Expr::Comparison {
        op: op.into(),
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn n() -> Expr {
    column(0, "n", DataType::Int64)
}
fn m() -> Expr {
    column(1, "m", DataType::Int64)
}
fn int(value: Option<i64>) -> Expr {
    Expr::Literal(Arc::new(Int64Array::from(vec![value])))
}

fn filters(schema: SchemaRef, expression: Expr) -> PushdownFilters {
    PushdownFilters {
        predicates: vec![Predicate {
            id: "p".into(),
            revision: 0,
            mode: PredicateMode::Required,
            expression,
        }],
        revisions: HashMap::new(),
        required_ids: HashSet::new(),
        context: EvaluationContext {
            profile: NO_CONTEXT.into(),
            time_zone: None,
            calendar: None,
            default_collation: None,
            ieee_floating_point_ops: None,
            integer_division: None,
            provider_fingerprint: None,
        },
        join_keys: Vec::new(),
        output_schema: schema,
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("m", DataType::Int64, true),
    ]))
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
            Arc::new(Int64Array::from(vec![None, Some(7), None])),
        ],
    )
    .unwrap()
}

fn mask(values: BooleanArray) -> Vec<bool> {
    assert_eq!(values.null_count(), 0);
    values.values().iter().collect()
}

fn contains(domain: &ColumnDomain, value: &ArrayRef) -> bool {
    if value.is_null(0) {
        return domain.may_be_null;
    }
    match &domain.non_null {
        NonNullDomain::Empty => false,
        NonNullDomain::All => true,
        NonNullDomain::Range { lower, upper } => {
            let value = Scalar::new(dictionary_values(value).unwrap());
            let lower_ok = match lower {
                Bound::Unbounded => true,
                Bound::Included(lower) => scalar_order(&value, lower).unwrap() != Ordering::Less,
                Bound::Excluded(lower) => scalar_order(&value, lower).unwrap() == Ordering::Greater,
            };
            let upper_ok = match upper {
                Bound::Unbounded => true,
                Bound::Included(upper) => scalar_order(&value, upper).unwrap() != Ordering::Greater,
                Bound::Excluded(upper) => scalar_order(&value, upper).unwrap() == Ordering::Less,
            };
            lower_ok && upper_ok
        }
    }
}

#[test]
fn nullable_or_never_discards_rows_with_unknown_payload() {
    let f = filters(
        schema(),
        Expr::Or(vec![
            cmp("eq", n(), int(Some(1))),
            Expr::IsNull {
                expression: Box::new(m()),
                negated: true,
            },
        ]),
    );
    assert_eq!(
        mask(f.may_match(&batch().project(&[0]).unwrap()).unwrap()),
        [true, true, true]
    );
    assert_eq!(mask(f.may_match(&batch()).unwrap()), [true, true, false]);
    let domain = f.column_domain("n").unwrap();
    assert!(domain.may_be_null());
    assert!(matches!(domain.non_null(), NonNullDomain::All));
}

#[test]
fn negation_preserves_unknown_instead_of_inverting_keep_mask() {
    let f = filters(
        schema(),
        Expr::Not(Box::new(Expr::Or(vec![
            cmp("eq", n(), int(Some(1))),
            cmp("eq", m(), int(Some(7))),
        ]))),
    );
    assert_eq!(
        mask(f.may_match(&batch().project(&[0]).unwrap()).unwrap()),
        [false, true, true]
    );
    assert_eq!(mask(f.may_match(&batch()).unwrap()), [false, false, false]);
}

#[test]
fn not_in_null_is_empty_but_empty_not_in_includes_null() {
    for (values, empty) in [(vec![Some(1), None], true), (vec![], false)] {
        let f = filters(
            schema(),
            Expr::In {
                expression: Box::new(n()),
                values: Arc::new(Int64Array::from(values)),
                negated: true,
            },
        );
        let domain = f.column_domain("n").unwrap();
        assert_eq!(domain.is_empty(), empty);
        assert_eq!(mask(f.may_match(&batch()).unwrap()), vec![!empty; 3]);
        if !empty {
            assert!(domain.may_be_null());
        }
    }
}

#[test]
fn typed_exclusive_endpoints_preserve_large_integer_decimal_and_timestamp_values() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![9_007_199_254_740_993])),
        Arc::new(UInt64Array::from(vec![u64::MAX])),
        Arc::new(
            Decimal128Array::from(vec![1234567890123456789_i128])
                .with_precision_and_scale(30, 7)
                .unwrap(),
        ),
        Arc::new(
            arrow_array::TimestampNanosecondArray::from(vec![i64::MAX])
                .with_timezone("America/New_York"),
        ),
        Arc::new(StringArray::from(vec!["zebra"])),
    ];
    for value in arrays {
        let dt = value.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new("value", dt.clone(), true)]));
        // Reversed: literal < value.
        let f = filters(
            schema,
            cmp(
                "lt",
                Expr::Literal(value.clone()),
                column(0, "value", dt.clone()),
            ),
        );
        let domain = f.column_domain("value").unwrap();
        assert_eq!(domain.data_type(), &dt);
        assert!(!domain.may_be_null());
        match domain.non_null() {
            NonNullDomain::Range {
                lower: Bound::Excluded(lower),
                upper: Bound::Unbounded,
            } => {
                assert_eq!(lower.clone().into_inner().to_data(), value.to_data());
            }
            other => panic!("unexpected domain: {other:?}"),
        }
    }
}

#[test]
fn contradictions_and_null_only_domains_are_distinct() {
    let contradictory = filters(
        schema(),
        Expr::And(vec![
            cmp("gt", n(), int(Some(2))),
            cmp("le", n(), int(Some(2))),
        ]),
    );
    assert!(contradictory.column_domain("n").unwrap().is_empty());
    let null_only = filters(schema(), cmp("not_distinct_from", n(), int(None)));
    let domain = null_only.column_domain("n").unwrap();
    assert!(!domain.is_empty());
    assert!(domain.may_be_null());
    assert!(matches!(domain.non_null(), NonNullDomain::Empty));
}

#[test]
fn malformed_bindings_error_and_zero_column_batches_retain_row_count() {
    let f = filters(schema(), cmp("eq", n(), int(Some(1))));
    let empty = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(3)),
    )
    .unwrap();
    assert_eq!(mask(f.may_match(&empty).unwrap()), [true, true, true]);
    assert!(f.column_domain("absent").is_err());
    let duplicate_values = Arc::new(Int64Array::from(vec![1]));
    let duplicates = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true); 2])),
        vec![duplicate_values; 2],
    )
    .unwrap();
    assert!(f.may_match(&duplicates).is_err());
    let wrong_type = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::UInt64, true)])),
        vec![Arc::new(UInt64Array::from(vec![1]))],
    )
    .unwrap();
    assert!(f.may_match(&wrong_type).is_err());
    let unknown = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "absent",
            DataType::Int64,
            true,
        )])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    assert!(f.may_match(&unknown).is_err());
    assert_eq!(f.may_match(&batch().slice(0, 0)).unwrap().len(), 0);
}

#[test]
fn every_exact_match_survives_every_known_column_subset_and_domain() {
    let values = [None, Some(-2), Some(-1), Some(0), Some(1), Some(2)];
    let full = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter(
                values.iter().flat_map(|n| values.iter().map(move |_| *n)),
            )),
            Arc::new(Int64Array::from_iter(values.iter().flat_map(|_| values))),
        ],
    )
    .unwrap();
    let mut atoms = vec![
        Expr::IsNull {
            expression: Box::new(n()),
            negated: false,
        },
        Expr::IsNull {
            expression: Box::new(m()),
            negated: true,
        },
        Expr::In {
            expression: Box::new(n()),
            values: Arc::new(Int64Array::from(vec![Some(-1), Some(1), None])),
            negated: true,
        },
        cmp(
            "ge",
            Expr::Literal(Arc::new(Int64Array::from(vec![1]))),
            n(),
        ),
    ];
    for op in [
        "eq",
        "ne",
        "lt",
        "le",
        "gt",
        "ge",
        "distinct_from",
        "not_distinct_from",
    ] {
        atoms.push(cmp(op, n(), int(Some(0))));
        atoms.push(cmp(op, m(), int(None)));
    }
    let mut expressions = atoms.clone();
    for a in &atoms {
        for b in &atoms {
            expressions.push(Expr::And(vec![a.clone(), b.clone()]));
            expressions.push(Expr::Or(vec![a.clone(), b.clone()]));
            expressions.push(Expr::Not(Box::new(Expr::Or(vec![a.clone(), b.clone()]))));
        }
    }
    for expression in expressions {
        let f = filters(schema(), expression);
        let exact = f.evaluate(&full).unwrap();
        for indices in [vec![], vec![0], vec![1], vec![1, 0]] {
            let partial = full.project(&indices).unwrap();
            let possible = f.may_match(&partial).unwrap();
            for (row, exact) in exact.iter().enumerate() {
                if exact == Some(true) {
                    assert!(
                        possible.value(row),
                        "false negative at row {row}, columns {indices:?}"
                    );
                }
            }
        }
        for (index, name) in ["n", "m"].iter().enumerate() {
            let domain = f.column_domain(name).unwrap();
            for (row, exact) in exact.iter().enumerate() {
                if exact == Some(true) {
                    assert!(
                        contains(&domain, &full.column(index).slice(row, 1)),
                        "domain omitted matching {name}, row {row}: {domain:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn float_equality_uses_sql_zero_and_nan_equivalence_classes() {
    // Independent expected results from DuckDB's documented standard ordering:
    // signed zeros equal; all NaNs equal; NaN sorts above infinity.
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(f64::from_bits(0xfff8000000000001)),
            Some(f64::from_bits(0x7ff8000000000002)),
            Some(f64::INFINITY),
            None,
        ])),
        Arc::new(Float32Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(f32::from_bits(0xffc00001)),
            Some(f32::from_bits(0x7fc00002)),
            Some(f32::INFINITY),
            None,
        ])),
    ];
    for values in arrays {
        let dt = values.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new("value", dt.clone(), true)]));
        let full = RecordBatch::try_new(schema.clone(), vec![values.clone()]).unwrap();
        for (op, literal_index, expected) in [
            ("eq", 0, vec![true, true, false, false, false, false]),
            ("eq", 2, vec![false, false, true, true, false, false]),
            ("gt", 4, vec![false, false, true, true, false, false]),
            (
                "not_distinct_from",
                3,
                vec![false, false, true, true, false, false],
            ),
        ] {
            let f = filters(
                schema.clone(),
                cmp(
                    op,
                    column(0, "value", dt.clone()),
                    Expr::Literal(values.slice(literal_index, 1)),
                ),
            );
            assert_eq!(mask(f.may_match(&full).unwrap()), expected);
            let domain = f.column_domain("value").unwrap();
            for (row, keep) in expected.into_iter().enumerate() {
                if keep {
                    assert!(contains(&domain, &values.slice(row, 1)));
                }
            }
        }
        let f = filters(
            schema,
            Expr::In {
                expression: Box::new(column(0, "value", dt)),
                values: values.slice(2, 1),
                negated: false,
            },
        );
        assert_eq!(
            mask(f.may_match(&full).unwrap()),
            [false, false, true, true, false, false]
        );
    }
}

#[test]
fn dictionary_bounds_decode_values_and_struct_fields_respect_parent_nulls() {
    use arrow_array::{types::Int8Type, DictionaryArray, Int8Array};
    let dict: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(1), Some(0), None]),
            Arc::new(StringArray::from(vec!["z", "a"])),
        )
        .unwrap(),
    );
    let dt = dict.data_type().clone();
    let schema = Arc::new(Schema::new(vec![Field::new("value", dt.clone(), true)]));
    let f = filters(
        schema.clone(),
        cmp(
            "eq",
            column(0, "value", dt),
            Expr::Literal(Arc::new(StringArray::from(vec!["a"]))),
        ),
    );
    assert_eq!(
        f.column_domain("value").unwrap().data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        mask(
            f.may_match(&RecordBatch::try_new(schema, vec![dict]).unwrap())
                .unwrap()
        ),
        [true, false, false]
    );

    let fields = vec![Arc::new(Field::new("child", DataType::Int64, true))];
    let parent: ArrayRef = Arc::new(StructArray::new(
        fields.clone().into(),
        vec![Arc::new(Int64Array::from(vec![1, 1]))],
        Some(NullBuffer::from(vec![true, false])),
    ));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "parent",
        parent.data_type().clone(),
        true,
    )]));
    let child = Expr::Field {
        expression: Box::new(column(0, "parent", parent.data_type().clone())),
        index: 0,
        name: "child".into(),
        data_type: DataType::Int64,
    };
    let f = filters(schema.clone(), cmp("eq", child, int(Some(1))));
    assert_eq!(
        mask(
            f.may_match(&RecordBatch::try_new(schema, vec![parent]).unwrap())
                .unwrap()
        ),
        [true, false]
    );
    assert!(matches!(
        f.column_domain("parent").unwrap().non_null(),
        NonNullDomain::All
    ));
}

#[test]
fn domain_membership_preserves_sql_float_and_logical_dictionary_nulls() {
    use arrow_array::{types::Int8Type, DictionaryArray, Int8Array};
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0, 1]),
            Arc::new(Int64Array::from(vec![None, Some(7)])),
        )
        .unwrap(),
    );
    let dt = dictionary.data_type().clone();
    let schema = Arc::new(Schema::new(vec![Field::new("n", dt.clone(), false)]));
    let f = filters(
        schema.clone(),
        Expr::IsNull {
            expression: Box::new(column(0, "n", dt)),
            negated: false,
        },
    );
    let batch = RecordBatch::try_new(schema, vec![dictionary.clone()]).unwrap();
    assert_eq!(mask(f.may_match(&batch).unwrap()), [true, false]);
    let domain = f.column_domain("n").unwrap();
    assert!(domain.may_be_null());
    assert!(!domain.is_empty());
    assert_eq!(
        mask(domain.may_contain(&dictionary).unwrap()),
        [true, false]
    );

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(f64::from_bits(0xfff8000000000001)),
        Some(f64::NAN),
        None,
    ]));
    for (index, expected) in [
        (0, vec![true, true, false, false, false]),
        (2, vec![false, false, true, true, false]),
    ] {
        let f = filters(
            schema.clone(),
            cmp(
                "eq",
                column(0, "x", DataType::Float64),
                Expr::Literal(values.slice(index, 1)),
            ),
        );
        let domain = f.column_domain("x").unwrap();
        assert_eq!(mask(domain.may_contain(&values).unwrap()), expected);
        assert!(domain
            .may_contain(&(Arc::new(Int64Array::from(vec![0])) as ArrayRef))
            .is_err());
    }
}

#[test]
fn unsupported_advisory_expression_cannot_partly_restrict_pruning() {
    let expression = Expr::And(vec![cmp("eq", n(), int(Some(1))), Expr::RuntimeFilter]);
    let mut f = filters(schema(), expression);
    f.predicates[0].mode = PredicateMode::Advisory;
    assert_eq!(mask(f.may_match(&batch()).unwrap()), [true, true, true]);
    assert!(matches!(
        f.column_domain("n").unwrap().non_null(),
        NonNullDomain::All
    ));
}

#[test]
fn removing_an_advisory_predicate_widens_fresh_domains_and_masks() {
    fn encode(json: &str) -> Vec<u8> {
        let metadata: HashMap<String, String> = [
            ("vgi_filter_encoding".into(), ENCODING.into()),
            ("vgi_filter_version".into(), VERSION.into()),
            ("vgi_evaluation_context".into(), NO_CONTEXT.into()),
        ]
        .into_iter()
        .collect();
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("filter_spec", DataType::Utf8, false)],
            metadata,
        ));
        ipc::write_batch(
            &RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![json]))]).unwrap(),
        )
        .unwrap()
    }
    let snapshot = encode(
        r#"{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"advisory","source":"query","expression":{"node":"is_null","expression":{"node":"column_ref","column_index":0,"column_name":"n"},"negated":false}}]}"#,
    );
    let mut f = PushdownFilters::parse_with_schema(&snapshot, schema()).unwrap();
    assert!(matches!(
        f.column_domain("n").unwrap().non_null(),
        NonNullDomain::Empty
    ));
    assert_eq!(mask(f.may_match(&batch()).unwrap()), [false, false, false]);
    let delta = encode(
        r#"{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"delta","updates":[{"operation":"remove","id":"p","revision":1}]}"#,
    );
    assert!(f.apply_delta(&delta).unwrap());
    assert!(matches!(
        f.column_domain("n").unwrap().non_null(),
        NonNullDomain::All
    ));
    assert_eq!(mask(f.may_match(&batch()).unwrap()), [true, true, true]);
}
