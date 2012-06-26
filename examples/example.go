package main

import (
    "github.com/ngmoco/gozk"
)

func main() {
    zk, session, err := gozk.Init("localhost:2181", 100e6) // 100ms
    if err != nil {
        println("Couldn't connect: " + err.String())
        return
    }

    defer zk.Close()

    // Wait for connection.
    event := <-session
    if event.State != gozk.STATE_CONNECTED {
        println("Couldn't connect")
        return
    }

    _, err = zk.Create("/counter", "0", 0, gozk.WorldACL(gozk.PERM_ALL))
    if err != nil {
        println(err.String())
    } else {
        println("Created!")
    }
}

