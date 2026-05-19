{# Build an s3:// path for `+materialized: external` outputs.

Wrapping the `{{ this }}` reference inside a macro means the project
config (`+location: "{{ external_path('silver') }}"`) does NOT itself
contain `{{ this }}`, which dbt-duckdb 1.10+ refuses to render at
project-parse time. The macro body is only evaluated when the
materialization runs, where `this` is bound.

Usage in dbt_project.yml:
  staging:
    +location: "{{ external_path('silver') }}"
  marts:
    +location: "{{ external_path('lakehouse_marts') }}"
#}
{% macro external_path(layer) %}
  {%- if layer == 'silver' -%}
    s3://{{ var('silver_bucket') }}/{{ this.identifier }}.parquet
  {%- elif layer == 'lakehouse_marts' -%}
    s3://{{ var('lakehouse_bucket') }}/marts/{{ this.identifier }}.parquet
  {%- else -%}
    {{ exceptions.raise_compiler_error("Unknown external_path layer: " ~ layer) }}
  {%- endif -%}
{% endmacro %}
