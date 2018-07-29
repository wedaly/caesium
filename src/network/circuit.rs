pub enum CircuitState {
    Closed,
    Open,
}

impl CircuitState {
    pub fn new() -> CircuitState {
        CircuitState::Closed
    }

    pub fn execute<F, G>(&self, mut if_closed_func: F, mut if_open_func: G)
    where
        F: FnMut(),
        G: FnMut(),
    {
        match self {
            CircuitState::Closed => if_closed_func(),
            CircuitState::Open => if_open_func(),
        }
    }
}
