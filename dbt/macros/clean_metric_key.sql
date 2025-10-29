{% macro clean_metric_key(expr) %}
lower(
  regexp_replace(
    regexp_replace(
      coalesce(nullif({{ expr }}, ''), 'unknown'),
      '[^a-zA-Z0-9]+', '_', 'g'
    ),
    '_+', '_', 'g'
  )
)
{% endmacro %}