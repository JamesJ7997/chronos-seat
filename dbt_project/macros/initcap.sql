{#
Converts a string to proper case (initial capitalization) while preserving the original punctuation, whitespace, and delimiter formatting. The macro identifies alphanumeric word tokens, capitalizes the first character of each word, lowercases the remaining characters, and then reconstructs the original string using the exact delimiters found between words.
#}

{% macro initcap(input_string) %}
    array_to_string(
        list_transform(
            range(
                1,
                array_length(regexp_extract_all({{ input_string }}, '[[:alnum:]]+')) + 1
            ),
            i ->
                upper(left(regexp_extract_all({{ input_string }}, '[[:alnum:]]+')[i], 1))
                || lower(substr(regexp_extract_all({{ input_string }}, '[[:alnum:]]+')[i], 2))
                || coalesce(
                    regexp_extract_all({{ input_string }}, '[^[:alnum:]]+')[i],
                    ''
                )
        ),
        ''
    )
{% endmacro %}
