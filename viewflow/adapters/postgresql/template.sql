
DROP TABLE IF EXISTS {{ params.schema }}.{{ params.task_id }}_tmp;
DROP TABLE IF EXISTS {{ params.schema }}.{{ params.task_id }}_old;

CREATE TABLE
    {{ params.schema }}.{{ params.task_id }}_tmp
    AS (
        {{ params.query }}
    );

-- If it's the first time a view is create, the first alter table in the transaction will fail because the table doesn't exist
CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.task_id }} (LIKE {{ params.schema }}.{{ params.task_id }}_tmp);

begin transaction;
ALTER TABLE {{ params.schema }}.{{ params.task_id }} RENAME TO {{ params.task_id }}_old;
ALTER TABLE {{ params.schema }}.{{ params.task_id }}_tmp RENAME TO {{ params.task_id }};
end transaction;

DROP TABLE {{ params.schema }}.{{ params.task_id }}_old;


{% if params.alias is not none %}
-- Create aliases

-- Create the tmp view
CREATE OR REPLACE VIEW {{ params.schema }}.{{ params.alias }} AS (SELECT * FROM {{ params.schema }}.{{ params.task_id }}) WITH NO SCHEMA BINDING;

{% endif %}

-- Comment the table
-- NOTE: Add more metadata: owner, tags, alias
COMMENT ON TABLE {{ params.schema }}.{{ params.task_id }} IS '{{ params.description }}';
{% for name, value in params.fields.items() %}
COMMENT ON COLUMN {{ params.schema }}.{{ params.task_id }}."{{ name }}" IS '{{ value }}';
{% endfor %}




