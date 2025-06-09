{%- macro load_tag_mapping() -%}
    {% set tag_mapping = get_tag_dictionary() %}
    {{ log("Loaded tag mappings: " ~ tag_mapping, info=True) }}
    {{ return(tag_mapping) }}
{%- endmacro -%}

{%- macro get_path_tags(model, additional_tags=[]) -%}
    {% set tags = [] %}

    {{ log(model.original_file_path, info=True) }}
    
    {% set path_str = model.original_file_path | string %}
    {% set path = path_str.split('/') %}
    
    {# Skip 'models' directory if it exists #}
    {% set start_index = 1 if path[0] == 'models' else 0 %}
    
    {# Process each directory in the path #}
    {% for part in path[start_index:-1] %}
        {% do tags.append(part) %}
    {% endfor %}
    
    {# Process the filename without extension #}
    {% set filename = path[-1] | replace('.sql', '') | replace('.yml', '') %}
    
    {# Split on __ and take the last part as the model name #}
    {% set name_parts = filename.split('__') %}
    {% if name_parts|length > 1 %}
        {# Add the prefix as a tag #}
        {% do tags.append(name_parts[0]) %}
        {# Add the actual model name #}
        {% do tags.append(name_parts[-1]) %}
    {% else %}
        {% do tags.append(filename) %}
    {% endif %}
    
    {# Add any additional tags provided #}
    {% if additional_tags is not none %}
        {% do tags.extend(additional_tags) %}
    {% endif %}
    
    {{ log("Initial tags: " ~ tags, info=True) }}
    
    {# Load tag mapping from YAML #}
    {% set tag_mapping = load_tag_mapping() %}
    
    {# Apply tag mapping rules #}
    {% set final_tags = tags.copy() %}
    {% for tag in tags %}
        {% if tag in tag_mapping %}
            {% do final_tags.extend(tag_mapping[tag]) %}
            {{ log("Added tags from '" ~ tag ~ "': " ~ tag_mapping[tag], info=True) }}
        {% endif %}
    {% endfor %}
    
    {{ log("Final tags: " ~ final_tags | unique | list, info=True) }}
    
    {# Return unique tags to avoid duplicates #}
    {{ return(final_tags | unique | list) }}
{%- endmacro -%}