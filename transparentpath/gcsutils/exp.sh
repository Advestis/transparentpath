#!/usr/bin/expect -f

spawn ssh [lindex $argv 0]@[lindex $argv 1]
#expect "yes/no"
#send "yes\r"

#set timeout 20
#expect "password: $"
#stty -echo
#read password
#send "$password\r"
interact
#expect "password: $"



