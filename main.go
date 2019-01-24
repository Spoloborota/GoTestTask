package main

func main() {
	c := Client{}
	c.Initialize("root", "123456", "testsdb")
	c.Run()
}
