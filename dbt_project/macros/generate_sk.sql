{#
  Generate surrogate key from natural key + effective date.
  Uses MD5 hash for deterministic, unique SKs.
#}

{% macro generate_sk(natural_key_column, date_column) %}
  md5(
    coalesce(cast({{ natural_key_column }} as varchar), '') || '-' ||
    coalesce(cast({{ date_column }} as varchar), '')
  )
{% endmacro %}