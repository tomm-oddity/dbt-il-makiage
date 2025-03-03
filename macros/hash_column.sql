{% macro hash_column(column_name, lower=True) -%}
    {%- if lower -%}
        sha2(lower({{ column_name }}), 256)
    {%- else -%}
        sha2({{ column_name }}, 256)
    {%- endif -%}
{%- endmacro %}