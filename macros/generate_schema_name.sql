{% macro generate_schema_name(
        custom_schema_name,
        node
    ) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {% if node.fqn [1:-2] | length == 0 %}
            {{ "dalgo" }}_{{ default_schema }}
        {% else %}
            {# Concat the subfolder(s) name to only #}
            {% set prefix = node.fqn [1:-2] | join('_') %}
            {{ "dalgo" }}_{{ prefix | trim }}
        {% endif %}
    {%- else -%}
        {{ "dalgo" }}_{{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}
