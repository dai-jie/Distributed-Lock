# Test CMD
Startup leader and follower

leader

```shell
# client 1
lock a
lock a
#client 2
lock a
unlock a
# client 1
unlock a
lock b
# client 3
check b
check b
```