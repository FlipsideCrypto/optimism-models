{% macro get_where_subquery(relation) -%}
    {%- set where = config.get('where') -%}
    
    {%- set interval_vars = namespace(
        interval_type = none,
        interval_value = none
    ) -%}
    
    {% set intervals = {
        'minutes': var('minutes', none),
        'hours': var('hours', none), 
        'days': var('days', none),
        'weeks': var('weeks', none),
        'months': var('months', none),
        'years': var('years', none)
    } %}
    
    {% for type, value in intervals.items() %}
        {% if value is not none %}
            {% set interval_vars.interval_type = type[:-1] %}
            {% set interval_vars.interval_value = value %}
            {% break %}
        {% endif %}
    {% endfor %}
    
    {% if 'dbt_expectations_expect_column_values_to_be_in_type_list' in this | string %}
        {% do return(relation) %}
    {% endif %}

    {%- set ts_vars = namespace(
        timestamp_column = none,
        filter_condition = none
    ) -%}

    {% if where and interval_vars.interval_type is not none and interval_vars.interval_value is not none %}
        {% if "__timestamp_filter__" in where %}
            {% set columns = adapter.get_columns_in_relation(relation) %}
            {% set column_names = columns | map(attribute='name') | list %}

            {% for column in columns %}
                {% if column.name == 'MODIFIED_TIMESTAMP' %}
                    {% set ts_vars.timestamp_column = 'MODIFIED_TIMESTAMP' %}
                    {% break %}
                {% endif %}
            {% endfor %}

            {% if not ts_vars.timestamp_column %}
                {% for column in columns %}
                    {% if column.name == '_INSERTED_TIMESTAMP' %}
                        {% set ts_vars.timestamp_column = '_INSERTED_TIMESTAMP' %}
                        {% break %}
                    {% endif %}
                {% endfor %}
            {% endif %}

            {% if not ts_vars.timestamp_column %}
                {% for column in columns %}
                    {% if column.name == 'BLOCK_TIMESTAMP' %}
                        {% set ts_vars.timestamp_column = 'BLOCK_TIMESTAMP' %}
                        {% break %}
                    {% endif %}
                {% endfor %}
            {% endif %}

            {% if ts_vars.timestamp_column is not none %}
                {% set ts_vars.filter_condition = ts_vars.timestamp_column ~ " >= dateadd(" ~ 
                    interval_vars.interval_type ~ ", -" ~ 
                    interval_vars.interval_value ~ ", current_timestamp())" %}
                {% set where = where | replace("__timestamp_filter__", ts_vars.filter_condition) %}
            {% endif %}
        {% endif %}
        
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }}) dbt_subquery
        {%- endset -%}
        {% do return(filtered) %}
    {%- else -%}
        {% do return(relation) %}
    {%- endif -%}
{%- endmacro %}