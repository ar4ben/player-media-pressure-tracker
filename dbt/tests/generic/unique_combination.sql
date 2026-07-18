{% test unique_combination(model, columns) %}
    select {{ columns | join(", ") }}
    from {{ model }}
    group by all
    having count(*) > 1
{% endtest %}
