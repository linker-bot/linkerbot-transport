# Classic CAN frame boundaries

Classic CAN dispatchers validate message structure and reject FD-only flags before
queue admission or physical transmission. Queued sends own a deep copy of the
message; callers must not mutate a message during the `send()` call. After it
returns, changes to the original cannot alter the queued frame. Valid classic
remote frames may be transmitted; received remote frames are excluded from
device routes, and malformed or FD frames follow receive error diagnostics.
