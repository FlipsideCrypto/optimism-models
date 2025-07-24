{% macro enable_change_tracking() %}
  {% if 'exclude_change_tracking' not in config.get('tags') and var('ENABLE_CHANGE_TRACKING', false) %}
    {% if config.get('materialized') == 'view' %}
      ALTER VIEW {{ this }} SET CHANGE_TRACKING = TRUE;
    {% else %}
      ALTER TABLE {{ this }} SET CHANGE_TRACKING = TRUE;
    {% endif %}
  {% endif %}
{% endmacro %}