#!/bin/bash
# smaple inputs

# export M_REQUESTS_PER_SEC=30
# export U_USERS=20
# export REQUEST_PER_USER=1200
# 2 min

# export M_REQUESTS_PER_SEC=10
# export U_USERS=6
# export REQUEST_PER_USER=4000
# 4 min

# export M_REQUESTS_PER_SEC=20
# export U_USERS=30
# export REQUEST_PER_USER=1000
# 3 min

export M_REQUESTS_PER_SEC=50
export U_USERS=10
export REQUEST_PER_USER=2000
# 1 min

go run goconcurrency.go