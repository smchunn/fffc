use csv::{ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::error::Error;
use uuid::Uuid;

#[derive(Debug)]
pub struct FFFC {
    pub lookup: HashMap<String, Uuid>,
    pub mains: HashMap<Uuid, String>,
    pub groups: HashMap<Uuid, Vec<String>>,
    pub links: HashMap<Uuid, Vec<Uuid>>,
}

impl FFFC {
    pub fn new() -> Self {
        FFFC {
            lookup: HashMap::new(),
            mains: HashMap::new(),
            groups: HashMap::new(),
            links: HashMap::new(),
        }
    }

    pub fn add_bw(&mut self, pn1: &str, pn2: &str) {
        if let Some(&id1) = self.lookup.get(pn1) {
            self.add_bw_by_id(id1, pn2);
        } else if let Some(&id2) = self.lookup.get(pn2) {
            self.add_bw_by_id(id2, pn1);
        } else {
            let id = self.add_part(pn1);
            self.add_bw_by_id(id, pn2);
        }
    }

    pub fn add_bw_by_id(&mut self, id: Uuid, pn: &str) {
        if let Some(&id_old) = self.lookup.get(pn) { // if pn in lookup
            if id != id_old { // merge 2 groups together
                if let Some(tmp_list) = self.groups.remove(&id_old) {
                    for tmp_pn in &tmp_list { // insert parts from old group in l
                        self.lookup.insert(tmp_pn.clone(), id.clone());
                    }
                    self.groups.entry(id).or_default().extend(tmp_list);
                    self.mains.remove(&id_old);
                }
                if let Some(links) = self.links.remove(&id_old) {
                    self.links.entry(id).or_default().extend(links)
                }
            }
        } else { // pn not in lookup -> add to groups, add to lookup
            self.groups.entry(id).or_default().push(pn.to_string());
            self.lookup.insert(pn.to_string(), id);
        }
    }

    pub fn add_ow(&mut self, pn_from: &str, pn_to: &str) {
        if let Some(&id_from) = self.lookup.get(pn_from) {
            if let Some(&id_to) = self.lookup.get(pn_to) {
                if let Some(resolved_links) = self.get_links(id_to) {
                    if resolved_links.contains(&id_from) {
                        self.add_bw(pn_from, pn_to);
                        for id in resolved_links {
                            if let Some(tmp_list) = self.groups.remove(&id) {
                                for tmp_pn in &tmp_list {
                                    self.lookup.insert(tmp_pn.clone(), id_from);
                                }
                                self.groups.entry(id_from).or_default().extend(tmp_list);
                                self.mains.remove(&id);
                            }
                        }
                    } else {
                        self.links.entry(id_from).or_insert_with(Vec::new).push(id_to);
                    }
                }
            } else {
                let id_to = self.add_part(pn_to);
                self.links.entry(id_from).or_insert_with(Vec::new).push(id_to);
            }
        } else if let Some(&id_to) = self.lookup.get(pn_to) {
            let id_from = self.add_part(pn_from);
            self.links.entry(id_from).or_insert_with(Vec::new).push(id_to);
        } else {
            let id_from = self.add_part(pn_from);
            let id_to = self.add_part(pn_to);
            self.links.entry(id_from).or_insert_with(Vec::new).push(id_to);
        }
    }

    pub fn add_part(&mut self, pn: &str) -> Uuid {
        if let Some(&id) = self.lookup.get(pn) {
            id
        } else {
            let id = Uuid::new_v4();
            self.groups.insert(id, vec![pn.to_string()]);
            self.lookup.insert(pn.to_string(), id);
            self.set_main(id, pn);
            id
        }
    }

    pub fn set_main(&mut self, id: Uuid, pn: &str) -> Uuid {
        if self.groups.contains_key(&id) {
            self.mains.insert(id, pn.to_string());
        } else {
            return self.add_part(pn);
        }
        id
    }

    pub fn get_links(&mut self, id: Uuid) -> Option<HashSet<Uuid>> {
        let mut link_set: HashSet<Uuid> = HashSet::new();
        let mut bfs_queue: LinkedList<Uuid> = LinkedList::new();

        bfs_queue.push_back(id);

        while let Some(cur_id) = bfs_queue.pop_front() {
            if link_set.contains(&cur_id) {
                if let Some(to_ids) = self.links.get(&cur_id) {
                    for tmp_id in to_ids {
                        link_set.insert(tmp_id.clone());
                        bfs_queue.push_back(*tmp_id);
                    }
                }
            }
        }
        Some(link_set)
    }
}

#[derive(Debug, Deserialize)]
struct CSVRecord {
    #[serde(rename = "MAIN")]
    main: String,
    #[serde(rename = "IC")]
    ic: String,
    #[serde(rename = "RELATIONSHIP")]
    relationship: u8,
}

#[derive(Debug, Serialize)]
struct FFFCRecord<'a> {
    part_number: &'a str,
    id: &'a Uuid,
}

#[derive(Debug, Serialize)]
struct FFFCMain<'a> {
    part_number: &'a str,
    id: &'a Uuid,
}

#[derive(Debug, Serialize)]
struct FFFCLink<'a> {
    id_from: &'a Uuid,
    id_to: &'a Uuid,
}

fn init_fffc_from_csv(fffc: &mut FFFC, filename: &str) -> Result<(), Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new().from_path(filename)?;
    for result in rdr.deserialize() {
        let record: CSVRecord = result.unwrap();
        match record.relationship {
            0 => _ = fffc.add_part(&record.main),
            1 => fffc.add_ow(&record.main, &record.ic),
            2 => fffc.add_bw(&record.main, &record.ic),
            _ => println!("Invalid relationship value: {}. Skipping row.", record.relationship),
        }
    }
    Ok(())
}

fn fffc_to_csv(fffc: &FFFC, path: &str) -> Result<(), Box<dyn Error>> {
    let mut wtr = WriterBuilder::new().from_path(format!("{path}/fffc_groups.csv"))?;
    for (part_number, id) in &fffc.lookup {
        wtr.serialize(FFFCRecord { part_number, id })?;
    }
    wtr.flush()?;
    let mut wtr = WriterBuilder::new().from_path(format!("{path}/fffc_mains.csv"))?;
    for (id, part_number) in &fffc.mains {
        wtr.serialize(FFFCMain { part_number, id })?;
    }
    wtr.flush()?;
    let mut wtr = WriterBuilder::new().from_path(format!("{path}/fffc_links.csv"))?;
    for (id_from, ids_to) in &fffc.links {
        for id_to in ids_to {
            wtr.serialize(FFFCLink { id_from, id_to })?;
        }
    }
    wtr.flush()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut fffc = FFFC::new();
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        println!("Usage: program <input_csv> <output_csv>");
        return Ok(());
    }
    init_fffc_from_csv(&mut fffc, &args[1])?;
    if let Err(_e) = std::fs::create_dir_all(&args[2]) {
        return Ok(());
    }
    fffc_to_csv(&fffc, &args[2])?;
    println!("Done");
    Ok(())
}

