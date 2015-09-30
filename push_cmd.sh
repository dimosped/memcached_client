#!/usr/bin/env bash

expect << EOF
spawn telnet 128.232.80.30 11211
send "$1\r"
expect -re "END\|ERROR"
send "^]"
EOF

