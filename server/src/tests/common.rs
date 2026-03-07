use std::sync::Once;

static INIT: Once = Once::new();

pub fn setup() {
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt::try_init();
    });
}
