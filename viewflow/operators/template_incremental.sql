
-- Create staging table with new rows for target table
DROP TABLE IF EXISTS {{ params.schema }}.{{ params.task_id }}_stage;
CREATE TABLE
    {{ params.schema }}.{{ params.task_id }}_stage
    AS (
        {{ params.query }}
    );

-- Create target table if necessary
CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.task_id }} (LIKE {{ params.schema }}.{{ params.task_id }}_stage);


BEGIN TRANSACTION;
-- Delete changed rows from target table
DELETE FROM {{ params.schema }}.{{ params.task_id }} AS target
USING {{ params.schema }}.{{ params.task_id }}_stage AS stage 
WHERE
    {% for column in params.primary_key %}
    {% if loop.index > 1 %}
    AND
    {% endif %}
    target.{{ column }} = stage.{{ column }}
    {% endfor %};

-- Insert all rows from staging table into target table
INSERT INTO {{ params.schema }}.{{ params.task_id }} 
    SELECT * FROM {{ params.schema }}.{{ params.task_id }}_stage;
END TRANSACTION;


DROP TABLE {{ params.schema }}.{{ params.task_id }}_stage;


-- Create aliases
{% if params.alias is not none %}
CREATE OR REPLACE VIEW {{ params.schema }}.{{ params.alias }} AS (SELECT * FROM {{ params.schema }}.{{ params.task_id }}) WITH NO SCHEMA BINDING;
{% endif %}

-- Comment the table
-- NOTE: Add more metadata: owner, tags, alias
COMMENT ON TABLE {{ params.schema }}.{{ params.task_id }} IS '{{ params.description }}';
{% for name, value in params.fields.items() %}
COMMENT ON COLUMN {{ params.schema }}.{{ params.task_id }}."{{ name }}" IS '{{ value }}';
{% endfor %}
