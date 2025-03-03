{% macro sanitize_email(email_column) %}
    -- Remove subaddressing from the local part
    REGEXP_REPLACE(
        -- Remove any comments (text in parentheses)
        REGEXP_REPLACE({{ email_column }}, '\\([^)]*\\)', ''),
        '^([^+]+)(\\+[^@]+)(@.+)$',
        '\\1\\3'
    )
{% endmacro %}