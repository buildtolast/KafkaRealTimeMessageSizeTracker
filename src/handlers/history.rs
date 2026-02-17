use crate::storage::TimeSeriesStorage;
use actix_web::{web, HttpResponse};

/// GET /api/history/{topic}?range=1h|6h|24h|7d — time-series data for a topic.
pub async fn get_history(
    path: web::Path<String>,
    query: web::Query<std::collections::HashMap<String, String>>,
    storage: web::Data<TimeSeriesStorage>,
) -> HttpResponse {
    let topic = path.into_inner();
    let range_str = query.get("range").map(|s| s.as_str()).unwrap_or("1h");

    let range_minutes = parse_range(range_str);

    let data = storage.query(&topic, range_minutes).await;
    HttpResponse::Ok().json(data)
}

/// Parse range string into minutes.
fn parse_range(range: &str) -> usize {
    match range {
        "1h" => 60,
        "6h" => 360,
        "24h" => 1440,
        "7d" => 10080,
        _ => {
            // Try parsing as raw minutes
            range.parse().unwrap_or(60)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_range() {
        assert_eq!(parse_range("1h"), 60);
        assert_eq!(parse_range("6h"), 360);
        assert_eq!(parse_range("24h"), 1440);
        assert_eq!(parse_range("7d"), 10080);
        assert_eq!(parse_range("120"), 120);
        assert_eq!(parse_range("invalid"), 60);
    }
}
