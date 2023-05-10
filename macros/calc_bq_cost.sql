{% macro calc_bq_on_demand_costs(total_billed_bytes) %}
    ( safe_divide({{ total_billed_bytes }}, pow(2,40)) * {{ var('leaner_query_bq_on_demand_pricing') }} )
{% endmacro %}

{% macro calc_bq_slot_based_costs(pricing_package, slot_time_ms) %}
    {# slot pricing #}
    {% set price_per_ms = var('leaner_query_bq_slot_pricing') %}
    (total_slot_ms) / ((1000*60*60) * {{ price_per_ms[pricing_package] }})
{% endmacro %}

{% macro calc_bq_cost(total_billed_bytes, slot_time_ms) %}
    {% if var('leaner_query_bq_pricing_schedule') == 'on_demand' %}
        {{ calc_bq_on_demand_costs(total_billed_bytes) }}
    {% else %}
        {{ calc_bq_slot_based_costs(var('leaner_query_bq_pricing_schedule'), slot_time_ms) }}
    {% endif %}
{% endmacro %}
