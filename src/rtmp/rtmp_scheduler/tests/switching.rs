//! Stream-switch and play-finished family tests.

use super::*;

// Pre-existing multi-play bug:
// a client that plays stream A and then plays stream B on the same
// connection must leave A's watcher set, stop receiving A's frames, and
// have its keyframe gate reset so B's deltas are withheld until B's keyframe.
#[test]
fn switching_streams_leaves_the_old_channel_and_regates_on_the_new_keyframe() {
    let mut scheduler = RtmpScheduler::new(10);
    let publisher_a_conn = 100;
    let watcher_conn = 2;

    assert!(scheduler.new_channel("stream_a".to_string(), publisher_a_conn));
    play(&mut scheduler, watcher_conn, "stream_a");
    let client_id = *scheduler
        .connection_to_client_map
        .get(&watcher_conn)
        .unwrap();
    assert!(scheduler
        .channels
        .get("stream_a")
        .unwrap()
        .watching_client_ids
        .contains(&client_id));

    // A's live keyframe reaches the watcher and opens its keyframe gate.
    let results = feed_video(&mut scheduler, "stream_a", 0, KEYFRAME);
    assert_eq!(results.len(), 1, "watcher should receive A's keyframe");
    assert!(
        scheduler
            .clients
            .get(client_id)
            .unwrap()
            .has_received_video_keyframe
    );

    // The same connection now plays stream B.
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        watcher_conn,
        2,
        "app".to_string(),
        "stream_b".to_string(),
        1,
        &mut results,
    );

    assert!(
        !scheduler
            .channels
            .get("stream_a")
            .unwrap()
            .watching_client_ids
            .contains(&client_id),
        "switching to B must remove the client from A's watcher set"
    );
    assert!(
        scheduler
            .channels
            .get("stream_b")
            .unwrap()
            .watching_client_ids
            .contains(&client_id),
        "the client must be a member of B's watcher set"
    );

    // The keyframe gate must have been reset by the new play request.
    assert!(
        !scheduler
            .clients
            .get(client_id)
            .unwrap()
            .has_received_video_keyframe,
        "a new play request must reset the keyframe gate"
    );

    // A's frames are no longer delivered to the switched client, and A's
    // keyframe must not re-open the gate for B.
    let results = feed_video(&mut scheduler, "stream_a", 33, KEYFRAME);
    assert!(
        results.is_empty(),
        "A's frames must not reach a client that switched to B"
    );
    assert!(
        !scheduler
            .clients
            .get(client_id)
            .unwrap()
            .has_received_video_keyframe,
        "A's live keyframe must not open the gate of a client watching B"
    );

    // B's delta frames are withheld until B's own keyframe arrives.
    let results = feed_video(&mut scheduler, "stream_b", 40, DELTA);
    assert!(
        results.is_empty(),
        "B's deltas must be withheld until B's keyframe"
    );
    let results = feed_video(&mut scheduler, "stream_b", 66, KEYFRAME);
    assert_eq!(results.len(), 1, "B's keyframe must be delivered");
    let results = feed_video(&mut scheduler, "stream_b", 100, DELTA);
    assert_eq!(results.len(), 1, "B's deltas flow after B's keyframe");
}

#[test]
fn switching_streams_gcs_the_empty_unpublished_old_channel() {
    let mut scheduler = RtmpScheduler::new(10);
    let watcher_conn = 2;

    // Play "stream_a" with no publisher: the channel is created on demand.
    play(&mut scheduler, watcher_conn, "stream_a");
    assert!(scheduler.channels.contains_key("stream_a"));

    // Switching to "stream_b" leaves "stream_a" empty and unpublished, so
    // the channel must be garbage collected.
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        watcher_conn,
        2,
        "app".to_string(),
        "stream_b".to_string(),
        1,
        &mut results,
    );
    assert!(
        !scheduler.channels.contains_key("stream_a"),
        "an empty, unpublished channel must be removed on stream switch"
    );
    assert!(scheduler.channels.contains_key("stream_b"));
}

#[test]
fn replaying_the_same_stream_keeps_the_watcher_membership() {
    let mut scheduler = RtmpScheduler::new(10);
    let publisher_conn = 100;
    let watcher_conn = 2;

    assert!(scheduler.new_channel("stream_a".to_string(), publisher_conn));
    play(&mut scheduler, watcher_conn, "stream_a");
    let client_id = *scheduler
        .connection_to_client_map
        .get(&watcher_conn)
        .unwrap();

    // A second play request for the same stream key must not drop the
    // membership (and must not GC the channel).
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        watcher_conn,
        2,
        "app".to_string(),
        "stream_a".to_string(),
        1,
        &mut results,
    );
    assert!(scheduler
        .channels
        .get("stream_a")
        .unwrap()
        .watching_client_ids
        .contains(&client_id));
}

// Pre-existing bug: PlayStreamFinished used to fall into the scheduler's
// catch-all arm, leaking the watcher membership and the keyframe gate.
#[test]
fn play_stream_finished_leaves_the_channel_and_resets_the_play_state() {
    let mut scheduler = RtmpScheduler::new(10);
    let publisher_conn = 100;
    let watcher_conn = 2;

    assert!(scheduler.new_channel("stream_a".to_string(), publisher_conn));
    play(&mut scheduler, watcher_conn, "stream_a");
    let client_id = *scheduler
        .connection_to_client_map
        .get(&watcher_conn)
        .unwrap();

    let results = feed_video(&mut scheduler, "stream_a", 0, KEYFRAME);
    assert_eq!(results.len(), 1, "watcher should receive the keyframe");
    assert!(
        scheduler
            .clients
            .get(client_id)
            .unwrap()
            .has_received_video_keyframe
    );

    // Deliver the finish through the real scheduler event path to prove
    // it no longer lands in the `_ => debug!` catch-all.
    let mut results = Vec::new();
    let _ = scheduler.handle_raised_event(
        watcher_conn,
        ServerSessionEvent::PlayStreamFinished {
            app_name: "app".to_string(),
            stream_key: "stream_a".to_string(),
        },
        &mut results,
    );

    let channel = scheduler.channels.get("stream_a").unwrap();
    assert!(
        !channel.watching_client_ids.contains(&client_id),
        "PlayStreamFinished must remove the watcher membership"
    );
    let client = scheduler.clients.get(client_id).unwrap();
    assert!(
        matches!(client.current_action, ClientAction::Waiting),
        "PlayStreamFinished must reset the action to Waiting"
    );
    assert!(
        !client.has_received_video_keyframe,
        "PlayStreamFinished must reset the keyframe gate"
    );

    // The published channel itself must survive, and its frames must no
    // longer be delivered to the finished client.
    let results = feed_video(&mut scheduler, "stream_a", 33, KEYFRAME);
    assert!(results.is_empty(), "no frames after the play finished");
}

#[test]
fn play_stream_finished_gcs_the_empty_unpublished_channel() {
    let mut scheduler = RtmpScheduler::new(10);
    let watcher_conn = 2;

    play(&mut scheduler, watcher_conn, "stream_a");
    assert!(scheduler.channels.contains_key("stream_a"));

    let mut results = Vec::new();
    let _ = scheduler.handle_raised_event(
        watcher_conn,
        ServerSessionEvent::PlayStreamFinished {
            app_name: "app".to_string(),
            stream_key: "stream_a".to_string(),
        },
        &mut results,
    );

    assert!(
        !scheduler.channels.contains_key("stream_a"),
        "an empty, unpublished channel must be removed when the play finishes"
    );
}

// A same-key replay collapses to one membership (single current_action),
// so a later PlayStreamFinished for that key stops the connection's current
// play. PlayStreamFinished carries no stream_id, so this key-only match is
// the only representable behaviour; pin it against regressions.
#[test]
fn play_stream_finished_after_a_same_stream_replay_tears_down_the_current_play() {
    let mut scheduler = RtmpScheduler::new(10);
    let publisher_conn = 100;
    let watcher_conn = 2;

    assert!(scheduler.new_channel("stream_a".to_string(), publisher_conn));
    play(&mut scheduler, watcher_conn, "stream_a");
    let client_id = *scheduler
        .connection_to_client_map
        .get(&watcher_conn)
        .unwrap();

    // Replay the SAME stream on a new stream_id (2). Membership is retained.
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        watcher_conn,
        2,
        "app".to_string(),
        "stream_a".to_string(),
        2,
        &mut results,
    );
    assert!(scheduler
        .channels
        .get("stream_a")
        .unwrap()
        .watching_client_ids
        .contains(&client_id));
    assert!(matches!(
        scheduler.clients.get(client_id).unwrap().current_action,
        ClientAction::Watching { stream_id: 2, .. }
    ));

    // A finish for "stream_a" stops the connection's (single) current play:
    // membership removed and action reset. The channel is NOT GC'd because
    // the publisher still holds it.
    let mut results = Vec::new();
    let _ = scheduler.handle_raised_event(
        watcher_conn,
        ServerSessionEvent::PlayStreamFinished {
            app_name: "app".to_string(),
            stream_key: "stream_a".to_string(),
        },
        &mut results,
    );
    assert!(!scheduler
        .channels
        .get("stream_a")
        .unwrap()
        .watching_client_ids
        .contains(&client_id));
    assert!(matches!(
        scheduler.clients.get(client_id).unwrap().current_action,
        ClientAction::Waiting
    ));
    // Publisher keeps the channel alive.
    assert!(scheduler.channels.contains_key("stream_a"));
}

#[test]
fn play_stream_finished_for_another_stream_is_ignored() {
    let mut scheduler = RtmpScheduler::new(10);
    let watcher_conn = 2;

    play(&mut scheduler, watcher_conn, "stream_b");
    let client_id = *scheduler
        .connection_to_client_map
        .get(&watcher_conn)
        .unwrap();

    // A finish for a stream the client is not watching must not disturb
    // the current play.
    let mut results = Vec::new();
    let _ = scheduler.handle_raised_event(
        watcher_conn,
        ServerSessionEvent::PlayStreamFinished {
            app_name: "app".to_string(),
            stream_key: "stream_a".to_string(),
        },
        &mut results,
    );

    let client = scheduler.clients.get(client_id).unwrap();
    assert!(matches!(
        client.current_action,
        ClientAction::Watching { ref stream_key, .. } if stream_key == "stream_b"
    ));
    assert!(scheduler
        .channels
        .get("stream_b")
        .unwrap()
        .watching_client_ids
        .contains(&client_id));
}
