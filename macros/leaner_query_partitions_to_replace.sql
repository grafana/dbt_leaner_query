{% macro leaner_query_partitions_to_replace(data_type) %}
  {#-
    Returns a list of partition expressions suitable for BigQuery's insert_overwrite `partitions` config.

    Controlled via vars:
      - leaner_query_microbatch_days (int): number of prior days to include
      - leaner_query_microbatch_include_today (bool): include current day partition

    `data_type` must be 'date' or 'timestamp'.
  -#}

  {% set days = var('leaner_query_microbatch_days', 3) | int %}
  {% set include_today = var('leaner_query_microbatch_include_today', true) %}

  {% if days < 0 %}
    {% set days = 0 %}
  {% endif %}

  {% set parts = [] %}

  {% if data_type == 'date' %}
    {% for i in range(days, 0, -1) %}
      {% do parts.append("date(date_add(current_date, interval -" ~ i ~ " day))") %}
    {% endfor %}
    {% if include_today %}
      {% do parts.append("date(current_date)") %}
    {% endif %}
  {% elif data_type == 'timestamp' %}
    {% for i in range(days, 0, -1) %}
      {% do parts.append("timestamp(timestamp_add(current_timestamp, interval -" ~ i ~ " day))") %}
    {% endfor %}
    {% if include_today %}
      {% do parts.append("timestamp(current_timestamp)") %}
    {% endif %}
  {% else %}
    {{ exceptions.raise_compiler_error("leaner_query_partitions_to_replace: data_type must be 'date' or 'timestamp'") }}
  {% endif %}

  {{ return(parts) }}
{% endmacro %}

