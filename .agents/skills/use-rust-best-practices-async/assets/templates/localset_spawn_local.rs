use std::rc::Rc;
use tokio::task::LocalSet;

pub async fn run_local_tasks() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let state = Rc::new("hello".to_owned());
            let state2 = state.clone();

            tokio::task::spawn_local(async move {
                println!("{state2}");
            })
            .await
            .expect("local task should finish");
        })
        .await;
}
