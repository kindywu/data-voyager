#![allow(unused)]

use std::sync::Arc;

use datafusion::{
    datasource::{provider_as_source, MemTable},
    error::Result,
    execution::context::SessionState,
    functions_aggregate::{
        count::count,
        expr_fn::{avg, median},
        stddev::stddev,
        sum::sum,
    },
    prelude::{max, min, DataFrame},
};

use datafusion_expr::{case, is_null, lit};

use datafusion::logical_expr::{col, LogicalPlanBuilder, UNNAMED_TABLE};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    compute::{cast, concat},
    datatypes::{DataType, Field, Schema},
};

async fn describe(df: DataFrame) -> Result<DataFrame> {
    //the functions now supported
    let supported_describe_functions =
        ["count", "null_count", "mean", "std", "min", "max", "median"];

    let original_schema_fields = df.schema().fields().iter();

    //define describe column
    let mut describe_schemas = vec![Field::new("describe", DataType::Utf8, false)];
    describe_schemas.extend(original_schema_fields.clone().map(|field| {
        if field.data_type().is_numeric() {
            Field::new(field.name(), DataType::Float64, true)
        } else {
            Field::new(field.name(), DataType::Utf8, true)
        }
    }));
    Ok(df)
}
