{% macro union_tables(table_name) %}
{% set years = var('years', []) %}  
{% set tables = [] %}
{% for year in years %}
    {% set full_table_name = source('fec', table_name ~ '_' ~ year) %}
    {% set tables = tables.append("(SELECT *, " ~ year ~ " as year FROM " ~ full_table_name ~ ")") %}
{% endfor %}
{{ return(tables | join(" UNION ALL ")) }}
{% endmacro %}
