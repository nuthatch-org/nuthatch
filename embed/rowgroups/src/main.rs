// RFC-0081 S0: for each footer `footers.mjs` saved, its row groups, and whether each one's min/max
// on the indexer column admits any of the given indexers; also whether that column has a bloom filter
// or a page index. Usage: cargo run --release -- <footer dir> <0xindexer,...>
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::statistics::Statistics;
use std::collections::BTreeMap;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = &args[1];
    let values: Vec<String> = args[2].split(',').map(|s| s.to_lowercase()).collect();
    #[derive(Default)]
    struct T {
        segs: u64,
        rgs: u64,
        admitted: u64,
        bytes: u64,
        admitted_bytes: u64,
        admitted_col_bytes: u64,
        no_stats: u64,
        bloom: u64,
        page_index: u64,
        col: String,
    }
    let mut by: BTreeMap<String, T> = BTreeMap::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        let parts: Vec<&str> = name.trim_end_matches(".footer").split('~').collect();
        let table = parts[0].to_string();
        let total: u64 = parts[2].parse().unwrap();
        let buf = std::fs::read(&p).unwrap();
        let md = ParquetMetaDataReader::decode_metadata(&buf).unwrap();
        let t = by.entry(table.clone()).or_default();
        t.segs += 1;
        t.bytes += total;
        let schema = md.file_metadata().schema_descr();
        let col = (0..schema.num_columns())
            .find(|&i| {
                let n = schema.column(i).name().to_string();
                n == "indexer" || n == "serviceProvider"
            })
            .expect("filter column");
        t.col = schema.column(col).name().to_string();
        for rg in md.row_groups() {
            t.rgs += 1;
            let c = rg.column(col);
            if c.bloom_filter_offset().is_some() {
                t.bloom += 1;
            }
            if c.column_index_offset().is_some() {
                t.page_index += 1;
            }
            let admit = match c.statistics() {
                Some(Statistics::ByteArray(s)) => match (s.min_opt(), s.max_opt()) {
                    (Some(lo), Some(hi)) => {
                        let lo = String::from_utf8_lossy(lo.data()).to_lowercase();
                        let hi = String::from_utf8_lossy(hi.data()).to_lowercase();
                        values.iter().any(|v| *v >= lo && *v <= hi)
                    }
                    _ => {
                        t.no_stats += 1;
                        true
                    }
                },
                other => {
                    if t.no_stats == 0 {
                        eprintln!("{table}: statistics {other:?}");
                    }
                    t.no_stats += 1;
                    true
                }
            };
            if admit {
                t.admitted += 1;
                t.admitted_bytes += rg.compressed_size() as u64;
                t.admitted_col_bytes += c.compressed_size() as u64;
            }
        }
    }
    let (mut s, mut r, mut a, mut b, mut ab) = (0, 0, 0, 0, 0);
    println!("table\tcolumn\tsegments\trow_groups\tadmitted\tbytes\tadmitted_rg_bytes\tno_stats\tbloom\tpage_index");
    for (k, t) in &by {
        println!(
            "{k}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            t.col,
            t.segs,
            t.rgs,
            t.admitted,
            t.bytes,
            t.admitted_bytes,
            t.no_stats,
            t.bloom,
            t.page_index
        );
        s += t.segs;
        r += t.rgs;
        a += t.admitted;
        b += t.bytes;
        ab += t.admitted_bytes;
    }
    println!("TOTAL\t\t{s}\t{r}\t{a}\t{b}\t{ab}");
}
