# SLCAN stream limits

The local SLCAN adapter limits partial ASCII lines to 143 bytes (including an
optional four-digit timestamp and terminator). An overlong line raises a
python-can operation error, then input is discarded through the next CR/bell.
Invalid encoding is consumed before it is reported, so following valid lines
can be received. Each low-level read processes at most 4096 bytes; these bounds
cover partial input as well as complete frames in classic and FD mode.
