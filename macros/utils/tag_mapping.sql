{%- macro get_tag_dictionary() -%}
    {% set tag_mapping = {
        
        'defi': ['curated', 'reorg'],
        'protocols': ['curated', 'reorg'],
        
        'silver': ['raw_data'],
        'gold': ['analytics_ready'],
        'core': ['core_tables']
    } %}
    
    {{ return(tag_mapping) }}
{%- endmacro -%}