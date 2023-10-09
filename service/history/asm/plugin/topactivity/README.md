# Top level activity Demo

This is a demo re. how top level activity can implemented on top of the plugin framework. The purpose is about the implementation, not the top level activity itself or the behavior of it.

To try out top activity plugin:

```
# Only cassandra is supported for now
make install-schema-es
make start-cass-es
```

```
# Run following commands in a separate terminal

# Register the "default" namespace
tctl n re 
```

## Demo for a successful run of top level activity
```
./tdbg topactivity create --id test-top-activity --at demo-at --task-queue demo-tq --payloads '{"Name": "Name"}'

./tdbg topactivity describe --id test-top-activity

./tdbg topactivity show --id test-top-activity

# The top level activity started by tdbg has a hardcoded 1min StartToCloseTimeout, so the following two commands needs to run within 1min.
./tdbg topactivity get-task --id test-top-activity

# Replace <token> with the token from the previous command output
./tdbg topactivity complete-task --id test-top-activity --task-token <token> --payloads '{"Result": "Completed"}'

./tdbg topactivity describe --id test-top-activity

./tdbg topactivity show --id test-top-activity
```

## Demo for top level activity retry
```
# NOTE: must use a different ID, ID reuse policy is not implemented yet.
./tdbg topactivity create --id test-top-activity-2 --at demo-at --task-queue demo-tq --payloads '{"Name": "Name"}'

./tdbg topactivity get-task --id test-top-activity-2

./tdbg topactivity fail-task --id test-top-activity-2 --task-token <token> --message "demo-failure-msg"

# The "state" and "attempt" fields should change
./tdbg topactivity describe --id test-top-activity-2
./tdbg topactivity describe --id test-top-activity-2

./tdbg topactivity show --id test-top-activity-2

./tdbg topactivity get-task --id test-top-activity-2

./tdbg topactivity complete-task --id test-top-activity-2 --task-token <token> --payloads '{"Result": "Completed"}'

./tdbg topactivity describe --id test-top-activity-2

./tdbg topactivity show --id test-top-activity-2
```