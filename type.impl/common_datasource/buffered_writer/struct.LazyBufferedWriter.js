(function() {var type_impls = {
"common_datasource":[["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-LazyBufferedWriter%3CT,+U,+F%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/common_datasource/buffered_writer.rs.html#47-73\">source</a><a href=\"#impl-LazyBufferedWriter%3CT,+U,+F%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;T: AsyncWrite + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Send.html\" title=\"trait core::marker::Send\">Send</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Unpin.html\" title=\"trait core::marker::Unpin\">Unpin</a>, U: <a class=\"trait\" href=\"common_datasource/buffered_writer/trait.DfRecordBatchEncoder.html\" title=\"trait common_datasource::buffered_writer::DfRecordBatchEncoder\">DfRecordBatchEncoder</a> + <a class=\"trait\" href=\"common_datasource/buffered_writer/trait.ArrowWriterCloser.html\" title=\"trait common_datasource::buffered_writer::ArrowWriterCloser\">ArrowWriterCloser</a>, F: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/function/trait.Fn.html\" title=\"trait core::ops::function::Fn\">Fn</a>(<a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/string/struct.String.html\" title=\"struct alloc::string::String\">String</a>) -&gt; Fut, Fut: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/future/future/trait.Future.html\" title=\"trait core::future::future::Future\">Future</a>&lt;Output = <a class=\"type\" href=\"common_datasource/error/type.Result.html\" title=\"type common_datasource::error::Result\">Result</a>&lt;T&gt;&gt;&gt; <a class=\"struct\" href=\"common_datasource/buffered_writer/struct.LazyBufferedWriter.html\" title=\"struct common_datasource::buffered_writer::LazyBufferedWriter\">LazyBufferedWriter</a>&lt;T, U, F&gt;</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.close_with_arrow_writer\" class=\"method\"><a class=\"src rightside\" href=\"src/common_datasource/buffered_writer.rs.html#56-72\">source</a><h4 class=\"code-header\">pub async fn <a href=\"common_datasource/buffered_writer/struct.LazyBufferedWriter.html#tymethod.close_with_arrow_writer\" class=\"fn\">close_with_arrow_writer</a>(self) -&gt; <a class=\"type\" href=\"common_datasource/error/type.Result.html\" title=\"type common_datasource::error::Result\">Result</a>&lt;(FileMetaData, <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u64.html\">u64</a>)&gt;</h4></section></summary><div class=\"docblock\"><p>Closes <code>LazyBufferedWriter</code> and optionally flushes all data to underlying storage\nif any row’s been written.</p>\n</div></details></div></details>",0,"common_datasource::file_format::parquet::InnerBufferedWriter"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-LazyBufferedWriter%3CT,+U,+F%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/common_datasource/buffered_writer.rs.html#75-167\">source</a><a href=\"#impl-LazyBufferedWriter%3CT,+U,+F%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;T: AsyncWrite + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Send.html\" title=\"trait core::marker::Send\">Send</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Unpin.html\" title=\"trait core::marker::Unpin\">Unpin</a>, U: <a class=\"trait\" href=\"common_datasource/buffered_writer/trait.DfRecordBatchEncoder.html\" title=\"trait common_datasource::buffered_writer::DfRecordBatchEncoder\">DfRecordBatchEncoder</a>, F: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/function/trait.Fn.html\" title=\"trait core::ops::function::Fn\">Fn</a>(<a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/string/struct.String.html\" title=\"struct alloc::string::String\">String</a>) -&gt; Fut, Fut: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/future/future/trait.Future.html\" title=\"trait core::future::future::Future\">Future</a>&lt;Output = <a class=\"type\" href=\"common_datasource/error/type.Result.html\" title=\"type common_datasource::error::Result\">Result</a>&lt;T&gt;&gt;&gt; <a class=\"struct\" href=\"common_datasource/buffered_writer/struct.LazyBufferedWriter.html\" title=\"struct common_datasource::buffered_writer::LazyBufferedWriter\">LazyBufferedWriter</a>&lt;T, U, F&gt;</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.close_inner_writer\" class=\"method\"><a class=\"src rightside\" href=\"src/common_datasource/buffered_writer.rs.html#83-88\">source</a><h4 class=\"code-header\">pub async fn <a href=\"common_datasource/buffered_writer/struct.LazyBufferedWriter.html#tymethod.close_inner_writer\" class=\"fn\">close_inner_writer</a>(&amp;mut self) -&gt; <a class=\"type\" href=\"common_datasource/error/type.Result.html\" title=\"type common_datasource::error::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.unit.html\">()</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Closes the writer without flushing the buffer data.</p>\n</div></details><section id=\"method.new\" class=\"method\"><a class=\"src rightside\" href=\"src/common_datasource/buffered_writer.rs.html#90-107\">source</a><h4 class=\"code-header\">pub fn <a href=\"common_datasource/buffered_writer/struct.LazyBufferedWriter.html#tymethod.new\" class=\"fn\">new</a>(\n    threshold: <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.usize.html\">usize</a>,\n    buffer: <a class=\"struct\" href=\"common_datasource/share_buffer/struct.SharedBuffer.html\" title=\"struct common_datasource::share_buffer::SharedBuffer\">SharedBuffer</a>,\n    encoder: U,\n    path: impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>&gt;,\n    writer_factory: F\n) -&gt; Self</h4></section><section id=\"method.write\" class=\"method\"><a class=\"src rightside\" href=\"src/common_datasource/buffered_writer.rs.html#109-118\">source</a><h4 class=\"code-header\">pub async fn <a href=\"common_datasource/buffered_writer/struct.LazyBufferedWriter.html#tymethod.write\" class=\"fn\">write</a>(&amp;mut self, batch: &amp;RecordBatch) -&gt; <a class=\"type\" href=\"common_datasource/error/type.Result.html\" title=\"type common_datasource::error::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.unit.html\">()</a>&gt;</h4></section><section id=\"method.try_flush\" class=\"method\"><a class=\"src rightside\" href=\"src/common_datasource/buffered_writer.rs.html#120-145\">source</a><h4 class=\"code-header\">pub async fn <a href=\"common_datasource/buffered_writer/struct.LazyBufferedWriter.html#tymethod.try_flush\" class=\"fn\">try_flush</a>(&amp;mut self, all: <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.bool.html\">bool</a>) -&gt; <a class=\"type\" href=\"common_datasource/error/type.Result.html\" title=\"type common_datasource::error::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u64.html\">u64</a>&gt;</h4></section><details class=\"toggle method-toggle\" open><summary><section id=\"method.maybe_init_writer\" class=\"method\"><a class=\"src rightside\" href=\"src/common_datasource/buffered_writer.rs.html#148-155\">source</a><h4 class=\"code-header\">async fn <a href=\"common_datasource/buffered_writer/struct.LazyBufferedWriter.html#tymethod.maybe_init_writer\" class=\"fn\">maybe_init_writer</a>(&amp;mut self) -&gt; <a class=\"type\" href=\"common_datasource/error/type.Result.html\" title=\"type common_datasource::error::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.reference.html\">&amp;mut T</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Only initiates underlying file writer when rows have been written.</p>\n</div></details><section id=\"method.try_flush_all\" class=\"method\"><a class=\"src rightside\" href=\"src/common_datasource/buffered_writer.rs.html#157-166\">source</a><h4 class=\"code-header\">async fn <a href=\"common_datasource/buffered_writer/struct.LazyBufferedWriter.html#tymethod.try_flush_all\" class=\"fn\">try_flush_all</a>(&amp;mut self) -&gt; <a class=\"type\" href=\"common_datasource/error/type.Result.html\" title=\"type common_datasource::error::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u64.html\">u64</a>&gt;</h4></section></div></details>",0,"common_datasource::file_format::parquet::InnerBufferedWriter"]]
};if (window.register_type_impls) {window.register_type_impls(type_impls);} else {window.pending_type_impls = type_impls;}})()