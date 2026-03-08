use crate::executor::column_batch::ColumnBatch;
use crate::executor::column_filter::eval_columnar_predicate;
use crate::executor::columnar_agg::ColumnarAggregator;
use crate::parser::ast::Expr;
use crate::tcop::engine::EngineError;

pub(crate) trait PipelineStage {
    fn push_batch(&mut self, batch: &ColumnBatch) -> Result<usize, EngineError>;
    fn finish(&mut self) -> Result<Option<ColumnBatch>, EngineError>;
}

pub(crate) struct FilterStage {
    predicate: Expr,
    next: Box<dyn PipelineStage>,
}

impl FilterStage {
    pub(crate) fn new(predicate: Expr, next: Box<dyn PipelineStage>) -> Self {
        Self { predicate, next }
    }
}

impl PipelineStage for FilterStage {
    fn push_batch(&mut self, batch: &ColumnBatch) -> Result<usize, EngineError> {
        let Some(mask) = eval_columnar_predicate(&self.predicate, batch) else {
            return Err(EngineError {
                message: "columnar predicate could not be evaluated in fused pipeline".to_string(),
            });
        };
        let filtered = batch.filter(&mask);
        if filtered.row_count == 0 {
            return Ok(0);
        }
        self.next.push_batch(&filtered)
    }

    fn finish(&mut self) -> Result<Option<ColumnBatch>, EngineError> {
        self.next.finish()
    }
}

pub(crate) struct ProjectStage {
    output_indices: Vec<usize>,
    next: Box<dyn PipelineStage>,
}

impl ProjectStage {
    pub(crate) fn new(output_indices: Vec<usize>, next: Box<dyn PipelineStage>) -> Self {
        Self {
            output_indices,
            next,
        }
    }
}

impl PipelineStage for ProjectStage {
    fn push_batch(&mut self, batch: &ColumnBatch) -> Result<usize, EngineError> {
        let projected = batch.project(&self.output_indices);
        if projected.row_count == 0 {
            return Ok(0);
        }
        self.next.push_batch(&projected)
    }

    fn finish(&mut self) -> Result<Option<ColumnBatch>, EngineError> {
        self.next.finish()
    }
}

pub(crate) struct AggregateSink {
    aggregator: ColumnarAggregator,
}

impl AggregateSink {
    pub(crate) fn new(aggregator: ColumnarAggregator) -> Self {
        Self { aggregator }
    }
}

impl PipelineStage for AggregateSink {
    fn push_batch(&mut self, batch: &ColumnBatch) -> Result<usize, EngineError> {
        self.aggregator.push_batch(batch)?;
        Ok(batch.row_count)
    }

    fn finish(&mut self) -> Result<Option<ColumnBatch>, EngineError> {
        let aggregator = std::mem::replace(
            &mut self.aggregator,
            ColumnarAggregator::new(Vec::new(), Vec::new(), Vec::new(), Vec::new()),
        );
        Ok(Some(aggregator.finish()?))
    }
}

pub(crate) struct LimitStage {
    remaining: usize,
    offset: usize,
    skipped: usize,
    next: Box<dyn PipelineStage>,
}

impl LimitStage {
    pub(crate) fn new(remaining: usize, offset: usize, next: Box<dyn PipelineStage>) -> Self {
        Self {
            remaining,
            offset,
            skipped: 0,
            next,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn is_exhausted(&self) -> bool {
        self.remaining == 0
    }
}

impl PipelineStage for LimitStage {
    fn push_batch(&mut self, batch: &ColumnBatch) -> Result<usize, EngineError> {
        if self.remaining == 0 {
            return Ok(0);
        }

        let available_after_offset = batch
            .row_count
            .saturating_sub(self.offset.saturating_sub(self.skipped));
        if available_after_offset == 0 {
            self.skipped = self.skipped.saturating_add(batch.row_count);
            return Ok(0);
        }

        let start = self
            .offset
            .saturating_sub(self.skipped)
            .min(batch.row_count);
        self.skipped = self.skipped.saturating_add(start);
        let take = available_after_offset.min(self.remaining);
        let limited = batch.slice(start, take);
        self.remaining -= limited.row_count;
        if limited.row_count == 0 {
            return Ok(0);
        }
        self.next.push_batch(&limited)
    }

    fn finish(&mut self) -> Result<Option<ColumnBatch>, EngineError> {
        self.next.finish()
    }
}

pub(crate) struct BatchCollector {
    batch: Option<ColumnBatch>,
    output_column_names: Vec<String>,
}

impl BatchCollector {
    pub(crate) fn new(output_column_names: Vec<String>) -> Self {
        Self {
            batch: None,
            output_column_names,
        }
    }
}

impl PipelineStage for BatchCollector {
    fn push_batch(&mut self, batch: &ColumnBatch) -> Result<usize, EngineError> {
        if let Some(existing) = &mut self.batch {
            existing
                .append_batch(batch)
                .map_err(|message| EngineError { message })?;
        } else {
            self.batch = Some(batch.clone());
        }
        Ok(batch.row_count)
    }

    fn finish(&mut self) -> Result<Option<ColumnBatch>, EngineError> {
        Ok(Some(self.batch.take().unwrap_or_else(|| {
            ColumnBatch::empty(self.output_column_names.clone())
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AggregateSink, BatchCollector, FilterStage, LimitStage, PipelineStage, ProjectStage,
    };
    use crate::executor::column_batch::ColumnBatch;
    use crate::executor::columnar_agg::{AggKind, AggSpec, ColumnarAggregator, OutputExpr};
    use crate::parser::ast::{BinaryOp, Expr};
    use crate::storage::tuple::ScalarValue;

    #[test]
    fn fuses_filter_and_project() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())],
            ],
            &["id".to_string(), "tag".to_string()],
        );
        let mut pipeline: Box<dyn PipelineStage> = Box::new(FilterStage::new(
            Expr::Binary {
                left: Box::new(Expr::Identifier(vec!["id".to_string()])),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Integer(1)),
            },
            Box::new(ProjectStage::new(
                vec![1],
                Box::new(BatchCollector::new(vec!["tag".to_string()])),
            )),
        ));

        pipeline
            .push_batch(&batch)
            .expect("pipeline should accept batch");
        let result = pipeline
            .finish()
            .expect("finish should succeed")
            .expect("collector should emit batch");

        assert_eq!(
            result.to_rows(),
            vec![vec![ScalarValue::Text("b".to_string())]]
        );
    }

    #[test]
    fn fuses_filter_and_aggregate() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Int(1), ScalarValue::Int(10)],
                vec![ScalarValue::Int(2), ScalarValue::Int(20)],
            ],
            &["dept".to_string(), "value".to_string()],
        );
        let aggregator = ColumnarAggregator::new(
            vec![0],
            vec![AggSpec {
                kind: AggKind::SumInt { column_index: 1 },
            }],
            vec![OutputExpr::GroupKey(0), OutputExpr::Aggregate(0)],
            vec!["dept".to_string(), "sum".to_string()],
        );
        let mut pipeline: Box<dyn PipelineStage> = Box::new(FilterStage::new(
            Expr::Binary {
                left: Box::new(Expr::Identifier(vec!["dept".to_string()])),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Integer(2)),
            },
            Box::new(AggregateSink::new(aggregator)),
        ));

        pipeline
            .push_batch(&batch)
            .expect("pipeline should accept batch");
        let result = pipeline
            .finish()
            .expect("finish should succeed")
            .expect("aggregate sink should emit batch");

        assert_eq!(
            result.to_rows(),
            vec![vec![ScalarValue::Int(2), ScalarValue::Int(20)]]
        );
    }

    #[test]
    fn limit_stage_supports_offset_and_early_stop() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Int(1)],
                vec![ScalarValue::Int(2)],
                vec![ScalarValue::Int(3)],
            ],
            &["id".to_string()],
        );
        let mut limit =
            LimitStage::new(1, 1, Box::new(BatchCollector::new(vec!["id".to_string()])));

        let emitted = limit
            .push_batch(&batch)
            .expect("limit stage should accept batch");
        let result = limit
            .finish()
            .expect("finish should succeed")
            .expect("collector should emit batch");

        assert_eq!(emitted, 1);
        assert!(limit.is_exhausted());
        assert_eq!(result.to_rows(), vec![vec![ScalarValue::Int(2)]]);
    }
}
