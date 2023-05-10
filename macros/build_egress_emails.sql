{% macro build_custom_egress_email_list() %}
    ({% for i in var('leaner_query_custom_egress_emails') %}
        '{{ i }}'
        {% if not loop.last %},{% endif %}
    {% endfor %})
{% endmacro %}

