{% macro determine_data_layer(table_name) %}
        case
        {% for stage_name in var('leaner_query_stage_dataset_names') %}
            when {{ table_name }} = '{{stage_name}}' then 'stage'
        {% endfor -%}
        {% for prod_name in var('leaner_query_prod_dataset_names') %}
            when {{ table_name }} like '{{prod_name}}' then 'prod'
        {% endfor %}
            when {{ table_name }} is null then 'None'
            else 'raw'
        end
{% endmacro %}
