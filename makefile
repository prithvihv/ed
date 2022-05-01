.PHONY: db_dumps

db_dump:
	pg_dump "postgres://postgres:postgres@localhost:5432/ed?sslmode=disable" > db_dumps/$(date +"%d-%m-%y").sql