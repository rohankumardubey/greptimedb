(function() {var implementors = {
"common_datasource":[["impl&lt;T: AsyncRead + AsyncSeek + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Unpin.html\" title=\"trait core::marker::Unpin\">Unpin</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Send.html\" title=\"trait core::marker::Send\">Send</a> + 'static&gt; RecordBatchStream for <a class=\"struct\" href=\"common_datasource/file_format/orc/struct.OrcArrowStreamReaderAdapter.html\" title=\"struct common_datasource::file_format::orc::OrcArrowStreamReaderAdapter\">OrcArrowStreamReaderAdapter</a>&lt;T&gt;"]],
"common_recordbatch":[["impl&lt;T: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Unpin.html\" title=\"trait core::marker::Unpin\">Unpin</a> + AsyncFileReader + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Send.html\" title=\"trait core::marker::Send\">Send</a> + 'static&gt; RecordBatchStream for <a class=\"struct\" href=\"common_recordbatch/adapter/struct.ParquetRecordBatchStreamAdapter.html\" title=\"struct common_recordbatch::adapter::ParquetRecordBatchStreamAdapter\">ParquetRecordBatchStreamAdapter</a>&lt;T&gt;"],["impl RecordBatchStream for <a class=\"struct\" href=\"common_recordbatch/adapter/struct.DfRecordBatchStreamAdapter.html\" title=\"struct common_recordbatch::adapter::DfRecordBatchStreamAdapter\">DfRecordBatchStreamAdapter</a>"]],
"promql":[["impl RecordBatchStream for <a class=\"struct\" href=\"promql/extension_plan/empty_metric/struct.EmptyMetricStream.html\" title=\"struct promql::extension_plan::empty_metric::EmptyMetricStream\">EmptyMetricStream</a>"],["impl RecordBatchStream for <a class=\"struct\" href=\"promql/extension_plan/instant_manipulate/struct.InstantManipulateStream.html\" title=\"struct promql::extension_plan::instant_manipulate::InstantManipulateStream\">InstantManipulateStream</a>"],["impl RecordBatchStream for <a class=\"struct\" href=\"promql/extension_plan/normalize/struct.SeriesNormalizeStream.html\" title=\"struct promql::extension_plan::normalize::SeriesNormalizeStream\">SeriesNormalizeStream</a>"],["impl RecordBatchStream for <a class=\"struct\" href=\"promql/extension_plan/histogram_fold/struct.HistogramFoldStream.html\" title=\"struct promql::extension_plan::histogram_fold::HistogramFoldStream\">HistogramFoldStream</a>"],["impl RecordBatchStream for <a class=\"struct\" href=\"promql/extension_plan/range_manipulate/struct.RangeManipulateStream.html\" title=\"struct promql::extension_plan::range_manipulate::RangeManipulateStream\">RangeManipulateStream</a>"],["impl RecordBatchStream for <a class=\"struct\" href=\"promql/extension_plan/series_divide/struct.SeriesDivideStream.html\" title=\"struct promql::extension_plan::series_divide::SeriesDivideStream\">SeriesDivideStream</a>"]],
"query":[["impl RecordBatchStream for <a class=\"struct\" href=\"query/range_select/plan/struct.RangeSelectStream.html\" title=\"struct query::range_select::plan::RangeSelectStream\">RangeSelectStream</a>"]]
};if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()