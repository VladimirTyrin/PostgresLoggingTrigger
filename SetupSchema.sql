CREATE SCHEMA IF NOT EXISTS trigger_test;

CREATE TABLE IF NOT EXISTS trigger_test.foo (
	id               BIGINT    NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	added_at         TIMESTAMP NOT NULL,
	unique_int_value INT       NOT NULL UNIQUE,
	int_value        INT       NULL,
	varchar_value    VARCHAR   NULL
);
