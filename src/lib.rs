use std::fmt::{Debug, Display};
use std::fs::{File, OpenOptions};
use std::path::Path;
use vector_map::VecMap as Map;

pub trait CSVFormatable {
    fn format(&self) -> String;
}

impl<T> CSVFormatable for T
where
    T: serde::Serialize,
{
    fn format(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

pub struct TableEntry<'l, K, V>
where
    K: PartialEq,
{
    map: &'l TableMap<K, V>,
    index: usize,
}

impl<'l, K, V> TableEntry<'l, K, V>
where
    K: PartialEq,
{
    pub fn keys<'t: 'l>(&'t self) -> Box<dyn Iterator<Item = &'t K> + 't> {
        self.map.columns.keys()
    }
    pub fn iter<'t: 'l>(&'t self) -> Box<dyn Iterator<Item = (&'t K, &'t V)> + 't> {
        let index = self.index;
        Box::new(self.map.columns.iter().filter_map(move |(key, column)| {
            if let Some(value) = &column[index] {
                Some((key, value))
            } else {
                None
            }
        }))
    }
    pub fn get<Lookup: PartialEq<K>>(&self, key: &Lookup) -> Option<&V> {
        if let Some(col) = self.map.columns.get(key) {
            col[self.index].as_ref()
        } else {
            None
        }
    }
}

impl<'l, K: PartialEq + Debug, V: Debug> Debug for TableEntry<'l, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{")?;
        for (key, value) in self.iter() {
            write!(f, "{:?}: {:?}, ", key, value)?;
        }
        write!(f, "}}")
    }
}

pub struct TableEntryMut<'l, K, V>
where
    K: PartialEq,
{
    map: &'l mut TableMap<K, V>,
    index: usize,
}

impl<'l, K, V> TableEntryMut<'l, K, V>
where
    K: PartialEq,
{
    pub fn keys<'t: 'l>(&'t self) -> Box<dyn Iterator<Item = &'t K> + 't> {
        self.map.columns.keys()
    }
    pub fn iter<'t: 'l>(&'t self) -> Box<dyn Iterator<Item = (&'t K, &'t V)> + 't> {
        let index = self.index;
        Box::new(self.map.columns.iter().filter_map(move |(key, column)| {
            if let Some(value) = &column[index] {
                Some((key, value))
            } else {
                None
            }
        }))
    }
    pub fn iter_mut<'t: 'l>(&'t mut self) -> Box<dyn Iterator<Item = (&'t K, &'t mut V)> + 't> {
        let index = self.index;
        Box::new(
            self.map
                .columns
                .iter_mut()
                .filter_map(move |(key, column)| {
                    if let Some(value) = &mut column[index] {
                        Some((key, value))
                    } else {
                        None
                    }
                }),
        )
    }

    pub fn get<Lookup: PartialEq<K>>(&self, key: &Lookup) -> Option<&V> {
        if let Some(col) = self.map.columns.get(key) {
            col[self.index].as_ref()
        } else {
            None
        }
    }

    pub fn get_mut<Lookup: PartialEq<K>>(&mut self, key: &Lookup) -> Option<&mut V> {
        if let Some(col) = self.map.columns.get_mut(key) {
            col[self.index].as_mut()
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let mut result = Some(value);
        std::mem::swap(&mut self.map.column_mut(key)[self.index], &mut result);
        result
    }
}

impl<'l, K: PartialEq + Debug, V: Debug> Debug for TableEntryMut<'l, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{")?;
        for (key, value) in self.iter() {
            write!(f, "{:?}: {:?}, ", key, value)?;
        }
        write!(f, "}}")
    }
}

#[derive(Clone)]
pub struct TableMap<K, V>
where
    K: PartialEq,
{
    columns: Map<K, Vec<Option<V>>>,
    len: usize,
}

impl<K, V> TableMap<K, V>
where
    K: PartialEq,
{
    pub fn new() -> Self {
        TableMap {
            columns: Default::default(),
            len: 0,
        }
    }

    fn column_mut(&mut self, key: K) -> &mut Vec<Option<V>> {
        let len = self.len();
        if self.columns.contains_key(&key) {
            self.columns.get_mut(&key).unwrap()
        } else {
            let inner = unsafe { self.columns.inner_mut() };
            let mut col = Vec::with_capacity(len);
            for _ in 0..len {
                col.push(None);
            }
            inner.push((key, col));
            inner.last_mut().map(|(_key, column)| column).unwrap()
        }
    }

    pub fn entry(&self, index: usize) -> TableEntry<K, V> {
        TableEntry { map: self, index }
    }

    pub fn entry_mut(&mut self, index: usize) -> TableEntryMut<K, V> {
        TableEntryMut { map: self, index }
    }

    pub fn entries(&self) -> impl Iterator<Item = TableEntry<K, V>> {
        (0..self.len()).map(move |i| self.entry(i))
    }

    pub fn new_entry(&mut self) -> &mut Self {
        self.len += 1;
        for (_key, column) in self.columns.iter_mut() {
            column.push(None)
        }
        self
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn keys<'l>(&'l self) -> Box<dyn Iterator<Item = &K> + 'l> {
        self.columns.keys()
    }

    pub fn last(&self) -> Option<TableEntry<K, V>> {
        if self.len() > 0 {
            Some(self.entry(self.len() - 1))
        } else {
            None
        }
    }

    pub fn last_mut(&mut self) -> Option<TableEntryMut<K, V>> {
        if self.len() > 0 {
            Some(self.entry_mut(self.len() - 1))
        } else {
            None
        }
    }

    pub fn remove_entry(&mut self, index: usize) {
        for (_, column) in unsafe { self.columns.inner_mut() } {
            column.remove(index);
        }
        self.len -= 1;
    }

    pub fn swap_remove_entry(&mut self, index: usize) {
        for (_, column) in unsafe { self.columns.inner_mut() } {
            column.swap_remove(index);
        }
        self.len -= 1;
    }

    pub fn cleanup(&mut self) {
        let mut marked = Vec::new();
        for (key, column) in unsafe { &*(self.columns.inner() as *const Vec<(K, Vec<Option<V>>)>) }
        {
            if column.iter().all(Option::is_none) {
                marked.push(key)
            }
        }
        for key in marked {
            self.columns.remove(key);
        }
        let mut line = 0;
        while line < self.len() {
            let entry = self.entry(line);
            if entry.iter().count() == 0 {
                self.remove_entry(line);
            } else {
                line += 1;
            }
        }
    }

    pub fn concatenate(&mut self, other: Self) -> &mut Self {
        let sl = self.len();
        let ol = other.len();
        for (key, value) in other.columns {
            match self.columns.inner().iter().position(|(k, _)| k == &key) {
                Some(i) => unsafe { self.columns.inner_mut()[i].1.extend(value) },
                None => unsafe {
                    self.columns.inner_mut().push((key, {
                        let mut values = Vec::with_capacity(sl + ol);
                        for _ in 0..sl {
                            values.push(None);
                        }
                        values.extend(value);
                        values
                    }))
                },
            }
        }
        self.len += ol;
        self
    }

    pub fn save_ssv<P>(&self, path: P) -> std::io::Result<()>
    where
        P: AsRef<Path>,
        K: Display,
        V: Display,
    {
        use std::io::Write;
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;
        write!(file, "{}", self)?;
        Ok(())
    }
}

impl TableMap<String, String> {
    pub fn load_ssv<P>(path: P) -> std::io::Result<Self>
    where
        P: AsRef<Path>,
    {
        use std::io::BufRead;
        let mut this = Self::new();
        let lines_count = std::io::BufReader::new(File::open(&path)?).lines().count();
        let mut lines = std::io::BufReader::new(File::open(path)?).lines();
        let keyline = lines.next().unwrap()?;
        let keys = keyline.split(';');
        for key in keys {
            unsafe {
                this.columns
                    .inner_mut()
                    .push((String::from(key), Vec::with_capacity(lines_count)))
            }
        }
        for line in lines {
            if let Ok(line) = line {
                if line.is_empty() {
                    continue;
                }
                for (i, value) in line.split(';').enumerate() {
                    unsafe { &mut this.columns.inner_mut()[i].1 }.push(if value.is_empty() {
                        None
                    } else {
                        Some(value.to_owned())
                    });
                }
                this.len += 1;
            }
        }
        Ok(this)
    }

    pub fn extract_json(&self) -> serde_json::Result<TableMap<String, serde_json::Value>> {
        use serde_json::from_str;
        let mut result = TableMap::new();
        for i in 0..self.len() {
            let mut entry = result.new_entry().last_mut().unwrap();
            for (key, value) in self.entry(i).iter() {
                entry.insert(key.clone(), from_str(value)?);
            }
        }
        Ok(result)
    }
}

impl<K: Display + PartialEq, V: Display> Display for TableMap<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys = self.columns.keys();
        if let Some(first_key) = keys.next() {
            write!(f, "{}", first_key)?;
            for key in keys {
                write!(f, ";{}", key)?;
            }
            writeln!(f)?;
            for i in 0..self.len() {
                let mut iterator = self.columns.iter();
                let col = iterator.next().unwrap().1;
                if let Some(value) = &col[i] {
                    write!(f, "{}", value)?;
                }
                for (_key, col) in iterator {
                    if let Some(value) = &col[i] {
                        write!(f, ";{}", value)?;
                    } else {
                        write!(f, ";")?;
                    }
                }
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SSVTable {
    pub table: TableMap<String, String>,
}

impl Display for SSVTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.table.fmt(f)
    }
}

impl Default for SSVTable {
    fn default() -> Self {
        Self::new()
    }
}

impl SSVTable {
    pub fn new() -> Self {
        SSVTable {
            table: TableMap::new(),
        }
    }

    pub fn keys<'l>(&'l self) -> Box<dyn Iterator<Item = &String> + 'l> {
        self.table.keys()
    }

    pub fn entry(&self, index: usize) -> CSVEntry {
        CSVEntry {
            inner: TableEntry {
                map: &self.table,
                index,
            },
        }
    }

    pub fn entry_mut(&mut self, index: usize) -> CSVEntryMut {
        CSVEntryMut {
            inner: TableEntryMut {
                map: &mut self.table,
                index,
            },
        }
    }

    pub fn new_entry(&mut self) -> &mut Self {
        self.table.new_entry();
        self
    }

    pub fn len(&self) -> usize {
        self.table.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn last(&self) -> Option<CSVEntry> {
        self.table.last().map(|entry| CSVEntry { inner: entry })
    }

    pub fn last_mut(&mut self) -> Option<CSVEntryMut> {
        self.table
            .last_mut()
            .map(|entry| CSVEntryMut { inner: entry })
    }

    pub fn remove_entry(&mut self, index: usize) {
        self.table.remove_entry(index)
    }

    pub fn swap_remove_entry(&mut self, index: usize) {
        self.table.swap_remove_entry(index)
    }

    pub fn cleanup(&mut self) {
        self.table.cleanup()
    }

    pub fn concatenate(&mut self, other: Self) -> &mut Self {
        self.table.concatenate(other.table);
        self
    }

    pub fn save_ssv<P>(&self, path: P) -> std::io::Result<()>
    where
        P: AsRef<Path>,
    {
        self.table.save_ssv(path)
    }

    pub fn load_ssv<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        TableMap::load_ssv(path).map(|table| SSVTable { table })
    }
}

pub struct CSVEntry<'l> {
    pub inner: TableEntry<'l, String, String>,
}

pub struct CSVEntryMut<'l> {
    pub inner: TableEntryMut<'l, String, String>,
}

impl<'l> CSVEntryMut<'l> {
    pub fn insert<K: Into<String>, V: CSVFormatable>(&mut self, key: K, value: V) {
        self.inner.insert(key.into(), value.format());
    }
}

#[test]
fn load() {
    let table = TableMap::load_ssv("test_load.ssv")
        .unwrap()
        .extract_json()
        .unwrap();
    print!("{}", table);
}

#[test]
fn clean() {
    let mut table = TableMap::load_ssv("test_load.ssv").unwrap();
    let table2 = TableMap::load_ssv("test_load.ssv").unwrap();
    table.remove_entry(2);
    println!("read:\n{}", &table);
    table.cleanup();
    println!("cleaned:\n{}", &table);
    table.concatenate(table2);
    println!("cleaned + read:\n{}", &table);
}

#[test]
fn save() {
    let mut table = SSVTable::new();
    {
        let mut entry = table.new_entry().last_mut().unwrap();
        entry.insert("firstname", "John");
        entry.insert("lastname", "Snow");
        entry.insert("profession", "Knower of Nothing");
    }
    {
        let mut entry = table.new_entry().last_mut().unwrap();
        entry.insert("profession", "Night King");
        entry.insert("alive", false);
    }
    add_entry!(table, {"firstname": "Michelle", "cats": 1}).insert("lost", true);
    add_entry!(table, {"firstname": "Daenyris", "profession": "Mad \"Queen\""});
    table.save_ssv("test_save.ssv").unwrap();
}

#[macro_export]
macro_rules! add_entry {
    ($table: expr, {$($key:tt: $value:expr),*}) => {{
        let mut entry = $table.new_entry().last_mut().unwrap();
        $(entry.insert($key, $value);)*
        entry
    }};
}

#[test]
fn bench() {
    let path = "big.ssv";
    let size = std::fs::metadata(path).unwrap().len();
    let start = std::time::Instant::now();
    let map = SSVTable::load_ssv(path).unwrap();
    let elapsed = start.elapsed().as_secs_f32();
    println!(
        "Parsed {:.2} MB from disk in {:.3}s: {:.1}MB/s",
        size as f32 / 1e6,
        elapsed,
        size as f32 / elapsed / 1e6
    );
    let start = std::time::Instant::now();
    let data = format!("{}", map);
    let elapsed = start.elapsed().as_secs_f32();
    println!(
        "Wrote {:.2} MB in RAM in {:.3}s: {:.1}MB/s",
        data.len() as f32 / 1e6,
        elapsed,
        data.len() as f32 / elapsed / 1e6
    );
    let start = std::time::Instant::now();
    map.save_ssv("big_write.ssv").unwrap();
    let elapsed = start.elapsed().as_secs_f32();
    println!(
        "Wrote {:.2} MB on disk in {:.3}s: {:.1}MB/s",
        data.len() as f32 / 1e6,
        elapsed,
        data.len() as f32 / elapsed / 1e6
    );
    std::fs::remove_file("big_write.ssv").unwrap();
}

#[test]
fn predicates() {
    let table = TableMap::load_ssv("test_load.ssv").unwrap();
    for entry in table.entries().filter(|entry| {
        if let Some(alive) = entry.get(&"alive") {
            if alive == "false" {
                false
            } else {
                true
            }
        } else {
            true
        }
    }) {
        println!("{:?}", entry);
    }
}
