.PHONY: full-network-build full-network primary-only clean

full-network-build:
	docker compose --profile all up -d --build --no-deps --scale addressingserver-backup=2 --scale chatserver=2 --scale client=0

full-network:
	docker compose --profile all up -d --no-deps --scale addressingserver-backup=2 --scale chatserver=2 --scale client=0

primary-only:
	docker compose up --build addressingserver

clean:
	docker compose down --volumes