use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::ProjectionMask;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::column::page::PageReader;
use std::fs::File;
use std::time::Instant;

fn med(mut v: Vec<f64>) -> f64 { v.sort_by(|a, b| a.partial_cmp(b).unwrap()); v[v.len() / 2] }

fn main() {
    let path = std::env::args().nth(1).unwrap();
    let n = 7;
    let r = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
    let md = r.metadata().clone();
    println!("row groups {} rows {}", md.num_row_groups(), md.file_metadata().num_rows());
    let ncols = md.file_metadata().schema_descr().num_columns();
    let (mut tp, mut ta) = (0.0, 0.0);
    // whole-file read time
    let t: Vec<f64> = (0..n).map(|_| { let s = Instant::now(); let b = std::fs::read(&path).unwrap(); std::hint::black_box(b.len()); s.elapsed().as_secs_f64() * 1e3 }).collect();
    println!("fs::read whole file (page cache): {:.1} ms", med(t));
    for c in 0..ncols {
        let name = md.file_metadata().schema_descr().column(c).name().to_string();
        let comp = md.row_group(0).column(c).compression();
        let bytes = md.row_group(0).column(c).compressed_size();
        let unc = md.row_group(0).column(c).uncompressed_size();
        // (1) page read + decompress only
        let t1: Vec<f64> = (0..n).map(|_| {
            let s = Instant::now();
            let rg = r.get_row_group(0).unwrap();
            let mut pr = rg.get_column_page_reader(c).unwrap();
            let mut pages = 0usize;
            while let Some(p) = pr.get_next_page().unwrap() { pages += p.buffer().len(); }
            std::hint::black_box(pages);
            s.elapsed().as_secs_f64() * 1e3 }).collect();
        // (2) full arrow decode of this column
        let t2: Vec<f64> = (0..n).map(|_| {
            let s = Instant::now();
            let f = File::open(&path).unwrap();
            let meta = ArrowReaderMetadata::load(&f, ArrowReaderOptions::new()).unwrap();
            let b = ParquetRecordBatchReaderBuilder::new_with_metadata(f, meta);
            let mask = ProjectionMask::roots(b.parquet_schema(), [c]);
            let rd = b.with_projection(mask).with_batch_size(65536).build().unwrap();
            let mut rows = 0; for x in rd { rows += x.unwrap().num_rows(); }
            std::hint::black_box(rows);
            s.elapsed().as_secs_f64() * 1e3 }).collect();
        let (p, a) = (med(t1), med(t2));
        tp += p; ta += a;
        println!("{:8} {:?} {:>6.1}MB->{:>6.1}MB  read+decompress {:>6.1} ms | full arrow {:>6.1} ms | decode+build {:>6.1} ms",
            name, comp, bytes as f64/1e6, unc as f64/1e6, p, a, a - p);
    }
    println!("TOTAL read+decompress {:.1} ms | full arrow {:.1} ms | decode+build {:.1} ms ({:.0}% of total)", tp, ta, ta - tp, (ta - tp) / ta * 100.0);
}
