{% macro get_prepare_database() %}
    {% if target.name == 'local' %}
        {{ return(env_var('DBT_SNOWFLAKE_DATABASE')) }}
    {% elif target.name == 'dev' %}
        {{ return('DEV_ENTERPRISE_PREPARE') }}
    {% elif target.name == 'prod' %}
        {{ return('PROD_ENTERPRISE_PREPARE') }}
    {% else %}
        {{ exceptions.raise_compiler_error("Unknown target name: " ~ target.name) }}
    {% endif %}
{% endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

{%- macro build_surrogate_key(field_list) -%}

{%- set default_null_value = '_dbt_null_' -%}
{%- set fields = [] -%}

{%- for field in field_list -%}

    {%- do fields.append(
        "coalesce(cast(" ~ field ~ " as " ~ dbt.type_string() ~ "), '" ~ default_null_value  ~"')"
    ) -%}

    {%- if not loop.last %}
        {%- do fields.append("'-'") -%}
    {%- endif -%}

{%- endfor -%}

{{ dbt.hash(dbt.concat(fields)) }}

{%- endmacro -%}