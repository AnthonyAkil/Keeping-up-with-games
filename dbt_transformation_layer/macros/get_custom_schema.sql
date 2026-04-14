{% macro generate_schema_name(custom_schema_name, node) -%}
{# If a custom schema is provided, use it as-is; otherwise use the target schema #}

  {%- set default_schema = target.schema -%}
  {%- if custom_schema_name is not none -%}

    {{ custom_schema_name | trim }}

  {%- else -%}

    {{ default_schema }}

  {%- endif -%}
{%- endmacro %}