{% macro get_metric_keys() %}
  {% set sql %}
    select distinct coalesce(nullif(statistic_key,''), statistic_name) as raw_key
    from {{ source('silver','staging_stats') }}
    where coalesce(nullif(statistic_key,''), statistic_name) is not null
    order by 1
  {% endset %}
  {% if execute %}
    {% set res = run_query(sql) %}
    {% set out = [] %}
    {% set seen_cols = {} %}
    {% for row in res.rows %}
      {% set raw = row[0] %}
      {# Create a clean column name #}
      {% set clean = modules.re.sub('[^a-z0-9]+', '_', raw|lower).strip('_') %}
      {% set base_col = 'stat_' ~ clean %}
      
      {# Handle duplicates by appending the original key hash #}
      {% if base_col in seen_cols %}
        {% set col = base_col ~ '_' ~ loop.index %}
      {% else %}
        {% set col = base_col %}
      {% endif %}
      {% do seen_cols.update({base_col: True}) %}
      
      {% do out.append({'raw_key': raw, 'col': col}) %}
    {% endfor %}
    {{ return(out) }}
  {% else %}
    {{ return([]) }}
  {% endif %}
{% endmacro %}