use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::{Arc, LazyLock, Mutex},
    time::Instant,
};

static PROFILERS: LazyLock<Mutex<Vec<Arc<Inner>>>> = LazyLock::new(|| {
    extern "C" fn drop_profilers() {
        println!("Dropping profilers.");
        for inner in &*PROFILERS.lock().unwrap() {
            inner.dump_data();
        }
    }

    unsafe {
        libc::atexit(drop_profilers);
    }

    Mutex::new(Vec::new())
});

struct Inner {
    name: &'static str,
    data: Mutex<Vec<f64>>,
}

impl Inner {
    fn dump_data(&self) {
        let ip = local_ip_address::local_ip().unwrap();
        let name = self.name;
        let mut data = self.data.lock().unwrap();
        if data.len() > 1 {
            data.remove(0);
        }
        let file_name = format!("shared/{}-{}.profiler", ip, name);

        if data.len() == 0 {
            let _ = std::fs::remove_file(&file_name);
            return;
        }

        let avg = data.iter().sum::<f64>() / data.len() as f64;
        let second_moment = data.iter().map(|entry| entry * entry).sum::<f64>();
        let std_dev = (second_moment / data.len() as f64 - avg * avg).sqrt();
        let min = data.iter().copied().fold(f64::INFINITY, f64::min);
        let max = data.iter().copied().fold(f64::NEG_INFINITY, f64::max);

        let file = File::create(&file_name).unwrap();
        let mut file = BufWriter::new(file);

        writeln!(file, "avg: {}", avg).unwrap();
        writeln!(file, "std_dev: {}", std_dev).unwrap();
        writeln!(file, "min: {}", min).unwrap();
        writeln!(file, "max: {}", max).unwrap();
        writeln!(file).unwrap();

        writeln!(file, "DATA").unwrap();
        for entry in data.iter() {
            writeln!(file, "{}", entry).unwrap();
        }
    }
}

pub struct Profiler {
    inner: Arc<Inner>,
}

impl Profiler {
    pub fn new(name: &'static str) -> Profiler {
        let inner = Arc::new(Inner {
            name,
            data: Mutex::new(Vec::new()),
        });

        PROFILERS.lock().unwrap().push(inner.clone());

        Profiler { inner }
    }

    pub fn start(&self) -> Measuring {
        Measuring {
            start_time: Instant::now(),
            profiler: self,
        }
    }
}

pub struct Measuring<'a> {
    start_time: Instant,
    profiler: &'a Profiler,
}

impl Drop for Measuring<'_> {
    fn drop(&mut self) {
        let stop_time = Instant::now();
        let elapsed = (stop_time - self.start_time).as_secs_f64() * 1000.;
        self.profiler.inner.data.lock().unwrap().push(elapsed);
    }
}
