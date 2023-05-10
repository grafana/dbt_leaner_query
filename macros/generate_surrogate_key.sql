{# Extracted majority of logic from dbt_utils.generate_surrogate_key macro
    into this macro to avoid collisions and dpendencies.
 #}

{%- macro generate_surrogate_key(field_list) -%}
    {% set default_null_value = '_leaner_query_surrogate_key_null_'%}

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
