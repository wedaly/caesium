pub const BUFSIZE: usize = 256;
pub const BUFCOUNT: usize = 8;
pub const MAX_LEVEL_CAPACITY: usize = BUFSIZE * BUFCOUNT;
pub const MIN_LEVEL_CAPACITY: usize = 8;
pub const CAPACITY_DECAY: f32 = 2.0 / 3.0;
