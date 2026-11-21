# python-can 通道标识与清理

Classic CAN and FD SLCAN constructors resolve `port@baud` before pool identity,
channel ownership and driver creation. `COM5@1000000` and `COM5` with
`bus_options={"tty_baudrate": 1000000}` identify the same settings; a suffix
that conflicts with an explicit serial baudrate raises `ValidationError`. The
default serial baudrate remains 115200. Other backend channels are unchanged.

Channel and native library references release an owner-specific token. Retrying
interrupted close cannot consume another user's reference. Physical shutdown
and reference cleanup are tracked separately, so an already closed handle can
finish claim cleanup without repeating its shutdown; failed native Exit remains
retryable. Claims are process local and do not prevent other processes opening
the same device.
