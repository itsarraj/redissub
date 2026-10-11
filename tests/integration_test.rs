use redissub::resp::{decode_resp, encode_command, RespValue};
use redissub::{OutputFormat, RedisConnection};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::test]
async fn test_auth_and_select() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024];

        // 1. Expect AUTH secret
        let n = socket.read(&mut buf).await.unwrap();
        let (frame, consumed) = decode_resp(&buf[..n]).unwrap().unwrap();
        assert_eq!(
            frame,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"AUTH".to_vec())),
                RespValue::BulkString(Some(b"secret".to_vec())),
            ]))
        );
        socket.write_all(b"+OK\r\n").await.unwrap();

        // 2. Expect SELECT 3
        let rem = &buf[consumed..n];
        let n2 = if rem.is_empty() {
            socket.read(&mut buf).await.unwrap()
        } else {
            buf.copy_within(consumed..n, 0);
            n - consumed
        };
        let (frame2, _) = decode_resp(&buf[..n2]).unwrap().unwrap();
        assert_eq!(
            frame2,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"SELECT".to_vec())),
                RespValue::BulkString(Some(b"3".to_vec())),
            ]))
        );
        socket.write_all(b"+OK\r\n").await.unwrap();
    });

    let conn = RedisConnection::connect(
        &addr.to_string(),
        Some("secret"),
        None,
        Some(3),
    )
    .await;

    assert!(conn.is_ok());
    server_task.await.unwrap();
}

#[tokio::test]
async fn test_pubsub_subscribe_and_receive() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024];

        // Expect SUBSCRIBE alerts
        let n = socket.read(&mut buf).await.unwrap();
        let (frame, _) = decode_resp(&buf[..n]).unwrap().unwrap();
        assert_eq!(
            frame,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"SUBSCRIBE".to_vec())),
                RespValue::BulkString(Some(b"alerts".to_vec())),
            ]))
        );

        // Send subscribe ack
        let ack = encode_command(&["subscribe", "alerts", "1"]);
        socket.write_all(&ack).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send published message
        let msg = encode_command(&["message", "alerts", "disk almost full"]);
        socket.write_all(&msg).await.unwrap();
    });

    let mut conn = RedisConnection::connect(&addr.to_string(), None, None, None)
        .await
        .unwrap();

    conn.subscribe(&["alerts".into()]).await.unwrap();
    let event = conn.read_pubsub_event().await.unwrap();

    assert_eq!(event.kind, "message");
    assert_eq!(event.channel.as_deref(), Some("alerts"));
    assert_eq!(event.payload, "disk almost full");

    let formatted = event.format(OutputFormat::Raw, None);
    assert_eq!(formatted, "disk almost full");

    server_task.await.unwrap();
}

#[tokio::test]
async fn test_pubsub_pattern_subscription() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024];

        let n = socket.read(&mut buf).await.unwrap();
        let (frame, _) = decode_resp(&buf[..n]).unwrap().unwrap();
        assert_eq!(
            frame,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"PSUBSCRIBE".to_vec())),
                RespValue::BulkString(Some(b"orders.*".to_vec())),
            ]))
        );

        let ack = encode_command(&["psubscribe", "orders.*", "1"]);
        socket.write_all(&ack).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        let pmsg = encode_command(&["pmessage", "orders.*", "orders.eu", "EUR 450"]);
        socket.write_all(&pmsg).await.unwrap();
    });

    let mut conn = RedisConnection::connect(&addr.to_string(), None, None, None)
        .await
        .unwrap();

    conn.psubscribe(&["orders.*".into()]).await.unwrap();
    let event = conn.read_pubsub_event().await.unwrap();

    assert_eq!(event.kind, "pmessage");
    assert_eq!(event.pattern.as_deref(), Some("orders.*"));
    assert_eq!(event.channel.as_deref(), Some("orders.eu"));
    assert_eq!(event.payload, "EUR 450");

    server_task.await.unwrap();
}

#[tokio::test]
async fn test_stream_xread() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024];

        // Read XREAD BLOCK 1000 STREAMS telemetry $
        let n = socket.read(&mut buf).await.unwrap();
        let (frame, _) = decode_resp(&buf[..n]).unwrap().unwrap();
        assert_eq!(
            frame,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"XREAD".to_vec())),
                RespValue::BulkString(Some(b"BLOCK".to_vec())),
                RespValue::BulkString(Some(b"1000".to_vec())),
                RespValue::BulkString(Some(b"STREAMS".to_vec())),
                RespValue::BulkString(Some(b"telemetry".to_vec())),
                RespValue::BulkString(Some(b"$".to_vec())),
            ]))
        );

        // Construct mock XREAD response:
        // [ ["telemetry", [ ["1690000000000-0", ["voltage", "3.3", "current", "120mA"]] ]] ]
        let resp_bytes = b"*1\r\n*2\r\n$9\r\ntelemetry\r\n*1\r\n*2\r\n$15\r\n1690000000000-0\r\n*4\r\n$7\r\nvoltage\r\n$3\r\n3.3\r\n$7\r\ncurrent\r\n$5\r\n120mA\r\n";
        socket.write_all(resp_bytes).await.unwrap();
    });

    let mut conn = RedisConnection::connect(&addr.to_string(), None, None, None)
        .await
        .unwrap();

    let events = conn
        .xread(&["telemetry".into()], &["$".into()], 1000)
        .await
        .unwrap();

    assert_eq!(events.len(), 1);
    let entry = &events[0];
    assert_eq!(entry.kind, "stream");
    assert_eq!(entry.stream.as_deref(), Some("telemetry"));
    assert_eq!(entry.id.as_deref(), Some("1690000000000-0"));
    let fields = entry.fields.as_ref().unwrap();
    assert_eq!(fields.get("voltage").map(|s| s.as_str()), Some("3.3"));
    assert_eq!(fields.get("current").map(|s| s.as_str()), Some("120mA"));

    server_task.await.unwrap();
}

#[tokio::test]
async fn test_stream_xread_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024];
        let _ = socket.read(&mut buf).await.unwrap();

        // Null array response for timeout
        socket.write_all(b"*-1\r\n").await.unwrap();
    });

    let mut conn = RedisConnection::connect(&addr.to_string(), None, None, None)
        .await
        .unwrap();

    let events = conn
        .xread(&["mystream".into()], &["$".into()], 500)
        .await
        .unwrap();

    assert!(events.is_empty());
    server_task.await.unwrap();
}

#[tokio::test]
async fn test_config_set_keyspace() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024];

        let n = socket.read(&mut buf).await.unwrap();
        let (frame, _) = decode_resp(&buf[..n]).unwrap().unwrap();
        assert_eq!(
            frame,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"CONFIG".to_vec())),
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"notify-keyspace-events".to_vec())),
                RespValue::BulkString(Some(b"KEA".to_vec())),
            ]))
        );
        socket.write_all(b"+OK\r\n").await.unwrap();
    });

    let mut conn = RedisConnection::connect(&addr.to_string(), None, None, None)
        .await
        .unwrap();

    let res = conn.enable_keyspace_events().await;
    assert!(res.is_ok());

    server_task.await.unwrap();
}
