pub type Id = Vec<u32>; 
pub type Range = (u32, u32);

pub fn get_combined_id(base: &Id, offset: u32) -> Id {
    let mut new_id = base.clone();
    new_id.push(offset);
    new_id
}