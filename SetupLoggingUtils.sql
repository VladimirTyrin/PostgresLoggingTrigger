CREATE SCHEMA IF NOT EXISTS logging;

CREATE OR REPLACE FUNCTION logging.get_log_table_name(table_name NAME)
	RETURNS VARCHAR
	LANGUAGE PLPGSQL
AS
$$
BEGIN
	RETURN table_name || '_change_log';
END;
$$;

CREATE OR REPLACE FUNCTION logging.set_application_user(application_user VARCHAR)
	RETURNS VOID
	LANGUAGE PLPGSQL
AS
$$
BEGIN
	PERFORM set_config('logging.application_user', application_user, TRUE);
END;
$$;

CREATE OR REPLACE FUNCTION logging.get_application_user()
	RETURNS VARCHAR
	LANGUAGE PLPGSQL
AS
$$
BEGIN
	RETURN current_setting('logging.application_user', TRUE);
END;
$$;

CREATE OR REPLACE FUNCTION logging.get_trigger_name(schema_name VARCHAR, table_name VARCHAR)
	RETURNS VARCHAR
	LANGUAGE PLPGSQL
AS
$$
BEGIN
	RETURN schema_name || '_' || table_name || '_change_log_trigger';
END;
$$;

CREATE OR REPLACE FUNCTION logging.has_bigint_id_column(schema_name VARCHAR, table_name VARCHAR)
    RETURNS BOOLEAN AS
$BODY$
DECLARE
    result bool;
BEGIN
    SELECT COUNT(*)
    INTO result
    FROM information_schema.columns c
    WHERE c.table_schema = $1
      AND c.table_name = $2
      AND c.column_name = 'id'
      AND c.data_type = 'bigint'
	  AND c.is_nullable = 'NO';
    RETURN result;
END
$BODY$
    LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION logging.log_row_changes()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS
$$
BEGIN
	IF TG_OP = 'INSERT' THEN
		EXECUTE FORMAT('INSERT INTO %I.%I (action_type, source_id, transaction_timestamp, application_name, database_user, application_user, changes_data) SELECT $1, $2, $3, $4, $5, $6, $7',
			TG_TABLE_SCHEMA,
			logging.get_log_table_name(TG_TABLE_NAME))
		USING
			1,
			NEW.id,
			transaction_timestamp(),
			current_setting('application_name'),
			session_user,
			logging.get_application_user(),
			json_agg(row_to_json(x)) FROM(SELECT post.key AS column_name, NULL AS pre_value, post.value AS post_value
				FROM jsonb_each(to_jsonb(NEW)) AS post) x;
	END IF;

	IF TG_OP = 'UPDATE' AND OLD.* IS DISTINCT FROM NEW.* THEN
		EXECUTE FORMAT('INSERT INTO %I.%I (action_type, source_id, transaction_timestamp, application_name, database_user, application_user, changes_data) SELECT $1, $2, $3, $4, $5, $6, $7',
			TG_TABLE_SCHEMA,
			logging.get_log_table_name(TG_TABLE_NAME))
		USING
			2,
			NEW.id,
			transaction_timestamp(),
			current_setting('application_name'),
			session_user,
			logging.get_application_user(),
			json_agg(row_to_json(x)) FROM(SELECT pre.key AS column_name, pre.value AS pre_value, post.value AS post_value
				FROM jsonb_each(to_jsonb(OLD)) AS pre
				CROSS JOIN jsonb_each(to_jsonb(NEW)) AS post
				WHERE pre.key = post.key AND pre.value IS DISTINCT FROM post.value) x;
	END IF;

	IF TG_OP = 'DELETE' THEN
		EXECUTE FORMAT('INSERT INTO %I.%I (action_type, source_id, transaction_timestamp, application_name, database_user, application_user, changes_data) SELECT $1, $2, $3, $4, $5, $6, $7',
			TG_TABLE_SCHEMA,
			logging.get_log_table_name(TG_TABLE_NAME))
		USING
			3,
			OLD.id,
			transaction_timestamp(),
			current_setting('application_name'),
			session_user,
			logging.get_application_user(),
			json_agg(row_to_json(x)) FROM(SELECT pre.key AS column_name, pre.value AS pre_value, NULL AS post_value
				FROM jsonb_each(to_jsonb(OLD)) AS pre) x;
	END IF;

    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION logging.enable_table_changes_logging(schema_name VARCHAR, table_name VARCHAR)
	RETURNS VOID
    LANGUAGE PLPGSQL
AS
$$
DECLARE
	trigger_name VARCHAR = logging.get_trigger_name(schema_name, table_name);
	log_table_name VARCHAR = logging.get_log_table_name(table_name::NAME);
	log_table_index_name VARCHAR = log_table_name || '_source_id';
BEGIN
	IF NOT logging.has_bigint_id_column(schema_name, table_name) THEN
        RAISE EXCEPTION 'Table %.% does not have non-nullable id column, changes logging is not supported', schema_name, table_name;
    END IF;

	EXECUTE FORMAT('
	DROP TRIGGER IF EXISTS %I ON %I.%I',
		trigger_name,
		schema_name,
		table_name);

	EXECUTE FORMAT('
	CREATE TRIGGER %I
		AFTER UPDATE OR INSERT OR DELETE
		ON %I.%I
		FOR EACH ROW
		EXECUTE PROCEDURE logging.log_row_changes();',
		trigger_name,
		schema_name,
		table_name);

	EXECUTE FORMAT('
	CREATE TABLE IF NOT EXISTS %I.%I (
		id                     BIGINT    NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
		source_id              BIGINT    NOT NULL,
		action_type            INT       NOT NULL,
		transaction_timestamp  TIMESTAMP NOT NULL,
		application_name       VARCHAR   NOT NULL,
		database_user          VARCHAR   NOT NULL,
		application_user       VARCHAR   NULL,
		changes_data           JSONB     NOT NULL
		);',
		schema_name,
		log_table_name);

	EXECUTE FORMAT('COMMENT ON COLUMN %I.%I.action_type IS ''1=INSERT, 2=UPDATE, 3=DELETE'';',
		schema_name,
		log_table_name);

	EXECUTE FORMAT('CREATE INDEX IF NOT EXISTS %I ON %I.%I (source_id);',
		log_table_index_name,
		schema_name,
		log_table_name);
END;
$$;

CREATE OR REPLACE FUNCTION logging.disable_table_changes_logging(schema_name VARCHAR, table_name VARCHAR)
	RETURNS VOID
    LANGUAGE PLPGSQL
AS
$$
DECLARE
	trigger_name VARCHAR = logging.get_trigger_name(schema_name, table_name);
BEGIN
	EXECUTE FORMAT('
	DROP TRIGGER IF EXISTS %I ON %I.%I',
		trigger_name,
		schema_name,
		table_name);
END;
$$;