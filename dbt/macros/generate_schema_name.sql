{#
  Override dbt's default schema naming so custom schemas are taken literally.

  Default behaviour is `<target.schema>_<custom_schema>` (e.g. `main_gold`).
  We want `gold` and `staging` as-is so the Python analysis layer can read
  `gold.fact_daily_price` directly. Falls back to target.schema when a model
  has no custom schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
