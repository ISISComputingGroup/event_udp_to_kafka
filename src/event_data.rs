use anyhow::bail;

/// Representation of event data after parsing from the raw UDP representation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventData {
    time_of_flight: Vec<i32>,
    pixel_id: Vec<i32>,
}

impl EventData {
    pub fn new(time_of_flight: Vec<i32>, pixel_id: Vec<i32>) -> anyhow::Result<EventData> {
        if time_of_flight.len() != pixel_id.len() {
            bail!("time-of-flight length does not match pixel ID length");
        }
        Ok(EventData {
            time_of_flight,
            pixel_id,
        })
    }

    /// true if the number of neutron events is zero.
    pub fn is_empty(&self) -> bool {
        self.time_of_flight.is_empty()
    }

    /// The number of neutron events
    pub fn len(&self) -> usize {
        self.time_of_flight.len()
    }

    /// The time-of-flight data for each neutron event
    pub fn time_of_flight(&self) -> &[i32] {
        &self.time_of_flight
    }

    /// The pixel ID for each neutron event
    pub fn pixel_id(&self) -> &[i32] {
        &self.pixel_id
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_data_allows_empty_data() {
        assert!(EventData::new(vec![], vec![]).is_ok());
    }

    #[test]
    fn test_event_data_allows_equal_length_data() {
        assert!(EventData::new(vec![123], vec![456]).is_ok());
    }

    #[test]
    fn test_event_data_rejects_unequal_length_data() {
        assert!(EventData::new(vec![123], vec![456, 789]).is_err());
    }

    #[test]
    fn test_event_data_length() {
        assert_eq!(
            EventData::new(vec![123, 456], vec![12, 34]).unwrap().len(),
            2
        );
    }

    #[test]
    fn test_event_data_is_empty() {
        assert!(EventData::new(vec![], vec![]).unwrap().is_empty());
        assert!(!EventData::new(vec![0], vec![0]).unwrap().is_empty());
    }
}
