NAME = commander_3.0.1.go

run:
	go run $(NAME)
example:
	echo "-nmd 8 10 4" | go run $(NAME)