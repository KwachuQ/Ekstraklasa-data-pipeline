{% macro normalize_period(expr) %}
case
  when lower({{ expr }}) in ('all','match','full time','ft','0','overall') then 'ALL'
  when lower({{ expr }}) in ('1st','first half','1h','period1','1') then '1ST'
  when lower({{ expr }}) in ('2nd','second half','2h','period2','2') then '2ND'
  else 'ALL'
end
{% endmacro %}