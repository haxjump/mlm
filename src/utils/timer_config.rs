use std::cell::Cell;
use std::time::Duration;

use crate::smr::smr_types::SMREvent;
use crate::DurationConfig;
use crate::{error::ConsensusError, ConsensusResult};

/// Mlm timer config.
#[derive(Debug, Clone)]
pub struct TimerConfig {
    interval: Cell<u64>,
    propose: (u64, u64),
    prevote: (u64, u64),
    precommit: (u64, u64),
    brake: (u64, u64),
}

impl TimerConfig {
    pub fn new(interval: u64) -> Self {
        TimerConfig {
            interval: Cell::new(interval),
            propose: (24, 10),
            prevote: (10, 10),
            precommit: (5, 10),
            brake: (3, 10),
        }
    }

    pub fn set_interval(&self, interval: u64) {
        self.interval.set(interval);
    }

    pub fn update(&mut self, config: DurationConfig) {
        self.propose = config.get_propose_config();
        self.prevote = config.get_prevote_config();
        self.precommit = config.get_precommit_config();
        self.brake = config.get_brake_config();
    }

    pub fn get_timeout(&self, event: SMREvent) -> ConsensusResult<Duration> {
        match event {
            SMREvent::NewRoundInfo { .. } => Ok(self.get_propose_timeout()),
            SMREvent::PrevoteVote { .. } => Ok(self.get_prevote_timeout()),
            SMREvent::PrecommitVote { .. } => Ok(self.get_precommit_timeout()),
            SMREvent::Brake { .. } => Ok(self.get_brake_timeout()),
            _ => Err(ConsensusError::TimerErr("No commit timer".to_string())),
        }
    }

    fn get_propose_timeout(&self) -> Duration {
        Duration::from_millis(self.interval.get() * self.propose.0 / self.propose.1)
    }

    fn get_prevote_timeout(&self) -> Duration {
        Duration::from_millis(self.interval.get() * self.prevote.0 / self.prevote.1)
    }

    fn get_precommit_timeout(&self) -> Duration {
        Duration::from_millis(self.interval.get() * self.precommit.0 / self.precommit.1)
    }

    fn get_brake_timeout(&self) -> Duration {
        Duration::from_millis(self.interval.get() * self.brake.0 / self.brake.1)
    }
}
