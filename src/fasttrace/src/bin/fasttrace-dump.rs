//! Convert a raw fasttrace snapshot file.
//!
//! ```text
//! fasttrace-dump trace.ftrc                 # Chrome/Perfetto JSON on stdout
//! fasttrace-dump trace.ftrc -o trace.json   # same, to a file
//! fasttrace-dump trace.ftrc --text          # one record per line
//! fasttrace-dump trace.ftrc --header        # header JSON (clock, registry, threads)
//! ```

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::process::ExitCode;

use fasttrace::Snapshot;

fn usage() -> ExitCode {
    eprintln!("usage: fasttrace-dump <file.ftrc>... [--text | --header] [-o <out>]");
    eprintln!("  several .ftrc files merge into one Perfetto JSON, one pid per file");
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let mut inputs: Vec<String> = Vec::new();
    let mut output: Option<String> = None;
    let mut mode = "chrome";
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--text" => mode = "text",
            "--header" => mode = "header",
            "--chrome" => mode = "chrome",
            "-o" | "--output" => match args.next() {
                Some(o) => output = Some(o),
                None => return usage(),
            },
            "-h" | "--help" => return usage(),
            _ if a.starts_with('-') => return usage(),
            _ => inputs.push(a),
        }
    }
    if inputs.is_empty() || (inputs.len() > 1 && mode != "chrome") {
        return usage();
    }
    match run(&inputs, output.as_deref(), mode) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("fasttrace-dump: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(inputs: &[String], output: Option<&str>, mode: &str) -> io::Result<()> {
    let snaps = inputs
        .iter()
        .map(|p| Snapshot::read_raw(BufReader::new(File::open(p)?)))
        .collect::<io::Result<Vec<_>>>()?;
    let out: Box<dyn Write> = match output {
        Some(p) => Box::new(BufWriter::new(File::create(p)?)),
        None => Box::new(BufWriter::new(io::stdout().lock())),
    };
    match mode {
        "text" => snaps[0].write_text(out),
        "header" => {
            let mut out = out;
            serde_json::to_writer_pretty(&mut out, &snaps[0].header())?;
            writeln!(out)?;
            out.flush()
        }
        _ => fasttrace::write_chrome_json_merged(&snaps, out),
    }
}
