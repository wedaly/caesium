use network::client::Client;
use network::daemon::state::MetricState;
use std::sync::mpsc::Receiver;
use time::window::TimeWindow;

pub fn sender_thread(mut client: Client, input: Receiver<MetricState>) {
    loop {
        match input.recv() {
            Ok(metric_state) => send_to_backend(metric_state, &mut client),
            Err(_) => {
                info!("Channel closed, stopping sender thread");
                break;
            }
        }
    }
}

fn send_to_backend(metric_state: MetricState, client: &mut Client) {
    let name = metric_state.metric_name;
    let window = TimeWindow::new(metric_state.window_start, metric_state.window_end);
    let sketch = metric_state.sketch;
    match client.insert(name, window, sketch) {
        Ok(_) => debug!("Sent metric to backend"),
        Err(err) => error!("Could not send metric to backend: {:?}", err),
    }
}
