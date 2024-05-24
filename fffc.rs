use csv::{ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::collections::VecDeque;
use std::error::Error;
use uuid::Uuid;

#[derive(Debug)]
pub struct FFFC {
    // lookup: HashMap to lookup a group id for a given part_number
    pub lookup: HashMap<String, Uuid>,
    // mains: HashMap to lookup a main part for a given group id
    pub mains: HashMap<Uuid, String>,
    // groups: HashMap returning vec of all parts for a given group id
    pub groups: HashMap<Uuid, Vec<String>>,
    // links: K: from_id, V: vector of to_ids
    // represents table of one-to-many from_ids to to_ids
    pub links: HashMap<Uuid, Vec<Uuid>>,
    // links: K: to_id, V: vector of from_ids
    // represents table of one-to-many from_ids to to_ids
    // used as a reverse lookup for `links`
    pub links_reverse: HashMap<Uuid, Vec<Uuid>>,
}

impl FFFC {
    pub fn new() -> Self {
        FFFC {
            lookup: HashMap::new(),
            mains: HashMap::new(),
            groups: HashMap::new(),
            links: HashMap::new(),
            links_reverse: HashMap::new(),
        }
    }

    pub fn add_bw(&mut self, pn1: &str, pn2: &str) {
        // if pn1 in fffc then add pn2 to pn1's group
        if let Some(&id1) = self.lookup.get(pn1) {
            self.add_bw_by_id(id1, pn2);
        // if pn2 in fffc then add pn1 to pn2's group
        } else if let Some(&id2) = self.lookup.get(pn2) {
            self.add_bw_by_id(id2, pn1);
        // if neither in fffc then add pn1 as new part 
        // and add pn2 to pn1's group
        } else {
            let id = self.add_part(pn1);
            self.add_bw_by_id(id, pn2);
        }
    }

    // add a both-way relationship 
    // id: id of existing fffc group
    // pn: part number to add to existing group
    pub fn add_bw_by_id(&mut self, id: Uuid, pn: &str) {
        // if pn exists in lookup
        if let Some(&id_old) = self.lookup.get(pn) { 
            // then if the old group != new group, merge the two
            // old group will be deleted after
            if id != id_old { 
                // get all parts from old group &
                // insert into lookup for new group
                if let Some(tmp_pn_list) = self.groups.remove(&id_old) {
                    for tmp_pn in &tmp_pn_list { 
                        self.lookup.insert(tmp_pn.clone(), id);
                    }
                    self.groups.entry(id).or_default().extend(tmp_pn_list);
                    self.mains.remove(&id_old);
                }
                // For one way links: update old_id in links & links_reverse
                if let Some(tmp_ow_from_list) = self.links_reverse.remove(&id_old) {
                    for tmp_ow_from_id in &tmp_ow_from_list { 
                        if let Some(tmp_ow_to_list) = self.links.get_mut(tmp_ow_from_id) {
                            for tmp_ow_to_id in tmp_ow_to_list.iter_mut(){
                                if tmp_ow_to_id == &id_old {
                                    *tmp_ow_to_id = id_old;
                                }
                            }
                        }
                    }
                    self.links_reverse.entry(id).or_default().extend(tmp_ow_from_list);
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
                        self.links_reverse.entry(id_to).or_insert_with(Vec::new).push(id_from);
                    }
                }
            } else {
                let id_to = self.add_part(pn_to);
                self.links.entry(id_from).or_insert_with(Vec::new).push(id_to);
                self.links_reverse.entry(id_to).or_insert_with(Vec::new).push(id_from);
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
        let mut bfs_queue: VecDeque<Uuid> = VecDeque::new();
        bfs_queue.push_back(id);

        while let Some(cur_id) = bfs_queue.pop_front() {
            if link_set.contains(&cur_id) {
                if let Some(to_ids) = self.links.get(&cur_id) {
                    for tmp_id in to_ids {
                        link_set.insert(*tmp_id);
                        bfs_queue.push_back(*tmp_id);
                    }
                }
            }
        }
        Some(link_set)
    }

    fn extend_from_csv(&mut self, filename: &str) -> Result<(), Box<dyn Error>> {
        let mut rdr = ReaderBuilder::new().from_path(filename)?;
        for result in rdr.deserialize() {
            let record: CSVRecord = result.unwrap();
            match record.relationship {
                0 => _ = self.add_part(&record.main),
                1 => self.add_ow(&record.main, &record.ic),
                2 => self.add_bw(&record.main, &record.ic),
                _ => println!("Invalid relationship value: {}. Skipping row.", record.relationship),
            }
        }
        Ok(())
    }

    pub fn deserialize(&mut self, path: &str) 
    -> Result<(), Box<dyn Error>> {
        let mut rdr = ReaderBuilder::new().from_path(format!("{path}/fffc_groups.csv"))?;
        for result in rdr.deserialize() {
            let record: FFFCRecord = match result {
                Ok(record) => record,
                Err(e) => return Err(Box::new(e)),
            };
            self.lookup.entry(record.part_number.clone()).or_insert(record.id);
            self.groups.entry(record.id).or_insert_with(Vec::new).push(record.part_number);
        }
        let mut rdr = ReaderBuilder::new().from_path(format!("{path}/fffc_mains.csv"))?;
        for result in rdr.deserialize() {
            let record: FFFCMain = match result {
                Ok(record) => record,
                Err(e) => return Err(Box::new(e)),
            };
            self.mains.entry(record.id).or_insert(record.part_number);
        }
        let mut rdr = ReaderBuilder::new().from_path(format!("{path}/fffc_links.csv"))?;
        for result in rdr.deserialize() {
            let record: FFFCLink = match result {
                Ok(record) => record,
                Err(e) => return Err(Box::new(e)),
            };
            self.links.entry(record.id_from).or_insert_with(Vec::new).push(record.id_to);
            self.links_reverse.entry(record.id_to).or_insert_with(Vec::new).push(record.id_from);
        }
        Ok(())
    }

    fn serialize(&self, path: &str) -> Result<(), Box<dyn Error>> {
        let mut wtr = WriterBuilder::new().from_path(format!("{path}/fffc_groups.csv"))?;
        for (part_number, id) in &self.lookup {
            wtr.serialize(FFFCRecord { part_number:part_number.clone(), id:*id })?;
        }
        wtr.flush()?;
        let mut wtr = WriterBuilder::new().from_path(format!("{path}/fffc_mains.csv"))?;
        for (id, part_number) in &self.mains {
            wtr.serialize(FFFCMain { part_number:part_number.clone(), id:*id })?;
        }
        wtr.flush()?;
        let mut wtr = WriterBuilder::new().from_path(format!("{path}/fffc_links.csv"))?;
        for (id_from, ids_to) in &self.links {
            for id_to in ids_to {
                wtr.serialize(FFFCLink { id_from:*id_from, id_to:*id_to })?;
            }
        }
        wtr.flush()?;
        Ok(())
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

#[derive(Debug, Serialize, Deserialize)]
struct FFFCRecord {
    part_number: String,
    id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
struct FFFCMain {
    part_number: String,
    id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
struct FFFCLink {
    id_from: Uuid,
    id_to: Uuid,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut fffc = FFFC::new();
    let args: Vec<String> = std::env::args().collect();
    match args.len() {
        3 => {
            if let Err(_e) = fffc.deserialize(&args[2]) {
                println!("existing fffc not found");
            }
            fffc.extend_from_csv(&args[1])?;
            if let Err(_e) = std::fs::create_dir_all(&args[2]) {
                return Ok(());
            }
            fffc.serialize(&args[2])?;
            println!("Done");
            Ok(())
        },
        4 => {
            if let Err(_e) = fffc.deserialize(&args[1]) {
                println!("existing fffc not found");
            }
            fffc.extend_from_csv(&args[2])?;
            if let Err(_e) = std::fs::create_dir_all(&args[3]) {
                return Ok(());
            }
            fffc.serialize(&args[2])?;
            println!("Done");
            Ok(())
        }
        _ => {
            println!("Usage: program <input_csv> <output_path> ");
            println!("Usage: program <input_path> <input_csv> <output_path> ");
            return Ok(());
        },
    }
}

