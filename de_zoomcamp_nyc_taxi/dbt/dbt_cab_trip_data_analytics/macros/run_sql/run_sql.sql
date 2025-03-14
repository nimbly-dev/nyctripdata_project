{% materialization run_sql, adapter='postgres' %}
    {%- if execute -%}
        {% do run_query(this.sql) %}
        {{ log("run_sql: Executed SQL", info=True) }}
    {%- endif -%}
    {{ return({}) }}
{% endmaterialization %}
