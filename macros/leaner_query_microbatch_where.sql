{% macro leaner_query_microbatch_where(column_name, data_type) %}
  {#-
    Returns a SQL predicate limiting rows to the current microbatch window.

    Vars:
      - leaner_query_microbatch_days (int)
      - leaner_query_microbatch_include_today (bool)

    `data_type`:
      - 'date': compares DATE(<column_name>)
      - 'timestamp': compares TIMESTAMP(<column_name>) truncated to day
  -#}

  {% set days = var('leaner_query_microbatch_days', 3) | int %}
  {% set include_today = var('leaner_query_microbatch_include_today', true) %}

  {% if days < 0 %}
    {% set days = 0 %}
  {% endif %}

  {% if data_type == 'date' %}
    {% if include_today %}
      date({{ column_name }}) >= date_sub(current_date, interval {{ days }} day)
    {% else %}
      date({{ column_name }}) >= date_sub(current_date, interval {{ days }} day)
      and date({{ column_name }}) < current_date
    {% endif %}
  {% elif data_type == 'timestamp' %}
    {% if include_today %}
      timestamp_trunc(cast({{ column_name }} as timestamp), day) >= timestamp_trunc(timestamp_sub(current_timestamp, interval {{ days }} day), day)
    {% else %}
      timestamp_trunc(cast({{ column_name }} as timestamp), day) >= timestamp_trunc(timestamp_sub(current_timestamp, interval {{ days }} day), day)
      and timestamp_trunc(cast({{ column_name }} as timestamp), day) < timestamp_trunc(current_timestamp, day)
    {% endif %}
  {% else %}
    {{ exceptions.raise_compiler_error("leaner_query_microbatch_where: data_type must be 'date' or 'timestamp'") }}
  {% endif %}
{% endmacro %}

