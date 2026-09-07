// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
// ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
// ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
// ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
// ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
// ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
// ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
// ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
// ┃ This file is part of the Perspective library, distributed under the terms ┃
// ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
// ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

#[cfg(feature = "axum-ws")]
mod internal {
    // use tokio::sync::Mutex;
    use std::error::Error;
    use std::sync::Arc;

    use perspective_client::{OnUpdateOptions, TableInitOptions, UpdateData, UpdateOptions};
    use perspective_server::LocalClient;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_two_sync_clients_receive_messages_on_update() -> Result<(), Box<dyn Error>> {
        let server = perspective::server::Server::new(None);
        let client1 = LocalClient::new(&server);
        let client2 = LocalClient::new(&server);
        // Local patch (VortexServer): JSON rows instead of CSV, because the
        // ConanCenter pre-built Arrow is built with `with_csv=False` and CSV
        // ingest is compiled out (see CLAUDE.md, "CSV support"); and
        // `TableInitOptions::default()` instead of a struct literal, which
        // upstream left stale after adding `page_to_disk` / `list_flatten`.
        let mut options = TableInitOptions::default();
        options.set_name("Table1");
        let table = client1
            .table(
                UpdateData::JsonRows(r#"[{"x":1,"y":2},{"x":3,"y":4}]"#.to_owned()).into(),
                options,
            )
            .await?;

        let table2 = client2.open_table("Table1".to_owned()).await?;
        let view = table2.view(None).await?;
        let result = Arc::new(Mutex::new(false));
        let _sub = view
            .on_update(
                {
                    let result = result.clone();
                    move |_| {
                        let result = result.clone();
                        async move { *result.lock().await = true }
                    }
                },
                OnUpdateOptions::default(),
            )
            .await?;

        table
            .update(
                UpdateData::JsonRows(r#"[{"x":5,"y":6}]"#.to_owned()),
                UpdateOptions::default(),
            )
            .await?;

        assert!(*result.lock().await);
        Ok(())
    }
}
