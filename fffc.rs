use csv::{ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
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
    pub links: HashMap<Uuid, HashSet<Uuid>>,
    // links: K: to_id, V: vector of from_ids
    // represents table of one-to-many from_ids to to_ids
    // used as a reverse lookup for `links`
    pub links_reverse: HashMap<Uuid, HashSet<Uuid>>,
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
                            tmp_ow_to_list.remove(&id_old);
                            tmp_ow_to_list.insert(id);
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
        if let Some(&id_from) = self.lookup.get(pn_from) { // pn_from in fffc
            if let Some(&id_to) = self.lookup.get(pn_to) { // both in fffc
                // get all ids that id_to points to
                // println!("circular links: {:?}",self.get_circular_links(id_to, id_from));
                if let Some(circular_links) = self.get_circular_links(id_to, id_from) {
                    // println!("circular links: {:?}", circular_links);
                    // back to id_from i.e. circular reference
                    // self.add_bw(pn_from, pn_to); // convert to both-way
                    for id in &circular_links {
                        if let Some(tmp_list) = self.groups.remove(&id) {
                            for tmp_pn in &tmp_list {
                                self.lookup.insert(tmp_pn.clone(), id_from);
                            }
                            self.groups.entry(id_from).or_default().extend(tmp_list);
                            self.mains.remove(&id);
                            // get ids that point to the old id and remove them
                            if let Some(rev_links) = self.links_reverse.remove(&id) {
                                for tmp_id_from in rev_links {
                                    if tmp_id_from != id_from && !&circular_links.contains(&tmp_id_from) {
                                        self.links_reverse.entry(id_from).or_default().insert(tmp_id_from);
                                    }
                                    self.links.entry(tmp_id_from).or_default().remove(&id);
                                    if tmp_id_from != id_from {
                                        self.links.entry(tmp_id_from).or_default().insert(id_from);
                                    }
                                }
                            }
                            if let Some(links) = self.links.remove(&id) {
                                for tmp_id_to in links {
                                    if tmp_id_to != id_from && !&circular_links.contains(&tmp_id_to){
                                        self.links.entry(id_from).or_default().insert(tmp_id_to);
                                    }
                                    self.links_reverse.entry(tmp_id_to).or_default().remove(&id);
                                    if tmp_id_to != id_from {
                                        self.links_reverse.entry(tmp_id_to).or_default().insert(id_from);
                                    }
                                }
                            }
                        }
                    }
                } 
                else { // add link
                    self.links.entry(id_from).or_insert_with(HashSet::new).insert(id_to);
                    self.links_reverse.entry(id_to).or_insert_with(HashSet::new).insert(id_from);
                }
            } else { // add pn_to 
                let id_to = self.add_part(pn_to);
                self.links.entry(id_from).or_insert_with(HashSet::new).insert(id_to);
                self.links_reverse.entry(id_to).or_insert_with(HashSet::new).insert(id_from);
            }
        // pn_to in fffc, add pn_from
        } else if let Some(&id_to) = self.lookup.get(pn_to) {
            let id_from = self.add_part(pn_from);
            self.links.entry(id_from).or_insert_with(HashSet::new).insert(id_to);
            self.links_reverse.entry(id_to).or_insert_with(HashSet::new).insert(id_from);
        // neither in fffc, add both
        } else {
            let id_from = self.add_part(pn_from);
            let id_to = self.add_part(pn_to);
            self.links.entry(id_from).or_insert_with(HashSet::new).insert(id_to);
            self.links_reverse.entry(id_to).or_insert_with(HashSet::new).insert(id_from);
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

    // gets all links that an id point to 
    pub fn get_circular_links(&mut self, id: Uuid, id_search: Uuid) -> Option<HashSet<Uuid>> {
        let mut visited: HashSet<Uuid> = HashSet::new();
        let mut path: VecDeque<Uuid> = VecDeque::new();
        let mut stack: VecDeque<Uuid> = VecDeque::new();
        stack.push_back(id);
        // println!("starting id: {:?}", id);
        // println!("search id: {:?}", id_search);
        // println!();

        while let Some(cur_id) = stack.pop_back() {
            // println!("cur_id: {:?}", cur_id);
            // println!("stack: {:?}", stack);
            // println!("visited: {:?}", visited);
            path.push_back(cur_id);
            // println!("path: {:?}", path);
            if let Some(to_ids) = self.links.get(&cur_id) {
                // println!("to_ids: {:?}", to_ids);
                for &tmp_id in to_ids {
                    if visited.insert(tmp_id) { // Only add if it wasn't already in the set
                        stack.push_back(tmp_id);
                    }
                    if tmp_id == id_search {
                        return Some(path.into_iter().collect())
                    }
                }
            }
            else {
                path.pop_back();
            }
            // println!();
        }
        None
    }

    fn extend_from_csv(&mut self, filename: &str) -> Result<(), Box<dyn Error>> {
        let mut rdr = ReaderBuilder::new().delimiter(b'|').from_path(filename)?;
        for result in rdr.deserialize() {
            let record: CSVRecord = match result {
                Ok(record) => record,
                Err(e) => {
                    return Err(Box::new(e));
                },
            };
            match record.relationship {
                0 => _ = self.add_part(&record.main),
                1 => self.add_ow(&record.main, &record.ic),
                2 => self.add_bw(&record.main, &record.ic),
                _ => println!("Invalid relationship value: {}. Skipping row.", record.relationship),
            }
            // println!();
            // println!("groups:");
            // for group in &self.groups {
            //     println!("{:?}", group);
            // }
            // println!();
            // println!("links:");
            // for link in &self.links {
            //     println!("{:?}", link);
            // }
            // println!();
            // println!("links (r):");
            // for link in &self.links_reverse {
            //     println!("{:?}", link);
            // }
            // println!()
        }
        Ok(())
    }

    pub fn deserialize(&mut self, path: &str) 
    -> Result<(), Box<dyn Error>> {
        let mut rdr = ReaderBuilder::new().delimiter(b'|').from_path(format!("{path}/fffc_groups.csv"))?;
        for result in rdr.deserialize() {
            let record: FFFCRecord = match result {
                Ok(record) => record,
                Err(e) => return Err(Box::new(e)),
            };
            self.lookup.entry(record.part_number.clone()).or_insert(record.group_id);
            self.groups.entry(record.group_id).or_insert_with(Vec::new).push(record.part_number);
        }
        let mut rdr = ReaderBuilder::new().delimiter(b'|').from_path(format!("{path}/fffc_mains.csv"))?;
        for result in rdr.deserialize() {
            let record: FFFCMain = match result {
                Ok(record) => record,
                Err(e) => return Err(Box::new(e)),
            };
            self.mains.entry(record.group_id).or_insert(record.part_number);
        }
        let mut rdr = ReaderBuilder::new().delimiter(b'|').from_path(format!("{path}/fffc_links.csv"))?;
        for result in rdr.deserialize() {
            let record: FFFCLink = match result {
                Ok(record) => record,
                Err(e) => return Err(Box::new(e)),
            };
            self.links.entry(record.id_from).or_insert_with(HashSet::new).insert(record.id_to);
            self.links_reverse.entry(record.id_to).or_insert_with(HashSet::new).insert(record.id_from);
        }
        Ok(())
    }

    fn serialize(&self, path: &str) -> Result<(), Box<dyn Error>> {
        let mut wtr = WriterBuilder::new().delimiter(b'|').from_path(format!("{path}/fffc_groups.csv"))?;
        for (part_number, id) in &self.lookup {
            wtr.serialize(FFFCRecord { part_number:part_number.clone(), group_id:*id })?;
        }
        wtr.flush()?;
        let mut wtr = WriterBuilder::new().delimiter(b'|').from_path(format!("{path}/fffc_mains.csv"))?;
        for (id, part_number) in &self.mains {
            wtr.serialize(FFFCMain { part_number:part_number.clone(), group_id:*id })?;
        }
        wtr.flush()?;
        let mut wtr = WriterBuilder::new().delimiter(b'|').from_path(format!("{path}/fffc_links.csv"))?;
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
    group_id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
struct FFFCMain {
    part_number: String,
    group_id: Uuid,
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

