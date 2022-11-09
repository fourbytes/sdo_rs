use rand::Rng;

#[must_use] pub fn generate_request_id() -> String {
    let mut rng = rand::thread_rng();
    let id = rng.gen::<f64>().to_string();
    let (_, id) = id.split_once('.').unwrap_or_default();
    format!("R_{id}_")
}
