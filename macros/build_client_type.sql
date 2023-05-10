{% macro build_client_type(custom_client) %}
    when 
    {% set and_criteria = false -%}
    {% if custom_client["user_agent"] is not none -%}
        caller_supplied_user_agent like '{{ custom_client["user_agent"] }}%' 
        {% set and_criteria = true -%}
    {% endif -%}
    {% if custom_client["principal_email"] is not none -%}
        {% if and_criteria -%}
            and
        {% endif -%}
        principal_email like '%{{custom_client["principal_email"]}}%' 
    {% endif -%}
    then '{{custom_client["client_name"]}}'
{% endmacro %}
