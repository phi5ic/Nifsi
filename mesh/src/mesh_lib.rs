
pub struct Node {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub public_key: [u8; 32],
    pub last_seen: std::time::Instant,
}

pub async fn start_mesh() {

}
