CREATE SCHEMA IF NOT EXISTS trigger_test;

CREATE TABLE IF NOT EXISTS trigger_test.first_table (
	id            BIGINT    NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	added_at      TIMESTAMP NOT NULL,
	int_value     INT       NULL,
	varchar_value VARCHAR   NULL
);
