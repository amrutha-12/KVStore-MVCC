package main

import "kvstore/kvstore"

func main() {
	servers := []string{"localhost:1234"}
	kvstore.StartServer(servers, 0)
	client := kvstore.MakeClerk(servers)

	putResult1 := client.Put("key1", "value1")
	putResult2 := client.Put("key2", "value2")
	getResult := client.Get("key1")
	putResult3 := client.Put("key1", "value3")

	print("Put Result: ", putResult1, "\n")
	print("Put Result: ", putResult2, "\n")
	print("Get Result: ", getResult, "\n")
	print("Put Result: ", putResult3, "\n")
}
