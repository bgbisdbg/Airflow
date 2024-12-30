create schema if not exists "logs";

create table if not exists
	logs.csv_to_dag (
		execution_datetime TIMESTAMP not null,
		event_datetime TIMESTAMP not null,
		event_name VARCHAR(30) not null,
		event_status VARCHAR(30)
);