use serde::Deserialize;

/// Information about a progressing download.
///
/// This reports the [Self::total] amount of operations to download, how many of them have already
/// been [Self::downloaded] and finally a [Self::fraction] indicating relative progress.
#[derive(Deserialize, Debug, Clone)]
pub struct ProgressCounters {
    /// How many operations need to be downloaded in total for the current donwload to complete.
    pub total: i64,
    /// How many operations, out of [Self::total], have already been downloaded.
    pub downloaded: i64,
}

impl ProgressCounters {
    /// The relative amount of [Self::total] to items in [Self::downloaded], as a number between
    /// `0.0` and `1.0` (inclusive).
    ///
    /// When this number reaches `1.0`, all changes have been received form the sync service.
    /// Actually applying these changes happens before the [ProgressCounters] field is cleared
    /// though, so progress can stay at `1.0` for a short while before completing.
    pub fn fraction(&self) -> f32 {
        match self.total {
            0 => 0.0,
            _ => (self.downloaded as f32) / (self.total as f32),
        }
    }
}
