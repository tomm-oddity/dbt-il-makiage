{% macro email_is_valid(email_field) %}
    {{ email_field }} ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
{% endmacro %}