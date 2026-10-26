pub trait Clock {
    fn unix_seconds(&self) -> u64;
}

pub struct SessionService<C> {
    clock: C,
}

impl<C: Clock> SessionService<C> {
    pub fn new(clock: C) -> Self {
        Self { clock }
    }

    pub fn issue_expiry(&self, ttl_seconds: u64) -> u64 {
        self.clock.unix_seconds() + ttl_seconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct FakeClock {
        now: u64,
    }

    impl Clock for FakeClock {
        fn unix_seconds(&self) -> u64 {
            self.now
        }
    }

    #[test]
    fn computes_expiry_from_fake_clock() {
        let service = SessionService::new(FakeClock { now: 1_700_000_000 });
        assert_eq!(service.issue_expiry(300), 1_700_000_300);
    }
}
