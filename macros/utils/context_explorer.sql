{# -- Save this as macros/utils/explore_context.sql
{% macro explore_context(var_name, detailed=false) %}

{# This will explore a specific variable or the entire context #}
{% if var_name == 'all' %}
    {% do log('=== EXPLORING ALL AVAILABLE CONTEXT VARIABLES ===', info=true) %}
    {% for key in context %}
        {% do log('VARIABLE: ' ~ key, info=true) %}
        {% if detailed %}
            {% if (context[key] is mapping) or (context[key] is iterable and context[key] is not string) %}
                {% do log('   TYPE: ' ~ context[key].__class__.__name__ if context[key].__class__ is defined else 'Complex type', info=true) %}
                {% do log('   STRUCTURE: Cannot display full structure (complex type)', info=true) %}
                
                {# Try to get some basic info for specific types #}
                {% if context[key] is mapping %}
                    {% do log('   KEYS: ' ~ context[key].keys() | list, info=true) %}
                {% endif %}
            {% else %}
                {% do log('   VALUE: ' ~ context[key], info=true) %}
            {% endif %}
        {% endif %}
    {% endfor %}
{% else %}
    {# This will explore a specific variable #}
    {% if context[var_name] is defined %}
        {% do log('=== EXPLORING VARIABLE: ' ~ var_name ~ ' ===', info=true) %}
        
        {# Try to determine the type #}
        {% do log('TYPE: ' ~ context[var_name].__class__.__name__ if context[var_name].__class__ is defined else 'Unknown type', info=true) %}
        
        {# If it's a mapping (dictionary-like), show keys #}
        {% if context[var_name] is mapping %}
            {% do log('KEYS: ' ~ context[var_name].keys() | list, info=true) %}
            
            {# If detailed is true, try to show values for each key #}
            {% if detailed %}
                {% for key in context[var_name].keys() %}
                    {% do log('  ' ~ key ~ ': ' ~ context[var_name][key], info=true) %}
                {% endfor %}
            {% endif %}
        {% endif %}
        
        {# If it's another iterable but not a string, list items #}
        {% if context[var_name] is iterable and context[var_name] is not string and context[var_name] is not mapping %}
            {% do log('ITEMS: ' ~ context[var_name] | list, info=true) %}
        {% endif %}
        
        {# If it's a simple value, show it #}
        {% if context[var_name] is not mapping and (context[var_name] is not iterable or context[var_name] is string) %}
            {% do log('VALUE: ' ~ context[var_name], info=true) %}
        {% endif %}
        
        {# Try to show common attributes for objects #}
        {% do log('ATTRIBUTES:', info=true) %}
        {% for attr in ['name', 'schema', 'database', 'identifier', 'alias', 'original_file_path', 'package_name', 'path', 'unique_id'] %}
            {% if context[var_name][attr] is defined %}
                {% do log('  ' ~ attr ~ ': ' ~ context[var_name][attr], info=true) %}
            {% endif %}
        {% endfor %}
    {% else %}
        {% do log('Variable "' ~ var_name ~ '" not found in context', info=true) %}
    {% endif %}
{% endif %}

{% endmacro %} #}