use serde::Serialize;
use std::ops::Range;

#[derive(Serialize, Debug, Clone)]
pub struct RangeStats<T> {
    pub min: T,
    pub max: T,
    pub count: u64,
}

impl<T> RangeStats<T>
where
    T: PartialOrd + Copy,
{
    pub fn collect(&mut self, value: T) {
        if self.min > value {
            self.min = value;
        }

        if self.max < value {
            self.max = value;
        }

        self.count += 1;
    }

    pub fn merge(&mut self, other: &Self) {
        if self.min > other.min {
            self.min = other.min;
        }

        if self.max < other.max {
            self.max = other.max;
        }

        self.count += other.count;
    }
}

impl<T> From<&Range<T>> for RangeStats<T>
where
    T: PartialOrd + Copy,
{
    fn from(src: &Range<T>) -> Self {
        Self {
            min: src.end,
            max: src.start,
            count: 0,
        }
    }
}
