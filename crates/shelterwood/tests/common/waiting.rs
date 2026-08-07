use shelterwood::{TaskDef, Tree};

pub fn task() -> TaskDef {
    TaskDef::new(|context| async move {
        context.shutdown_token().cancelled().await;
        Ok(())
    })
}

pub fn tree() -> Tree {
    let mut tree = Tree::new();
    tree.add_task("worker", task()).expect("valid task");
    tree
}
