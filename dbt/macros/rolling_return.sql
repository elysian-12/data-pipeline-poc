{# Rolling return over the trailing `window_days` *calendar* days for a column #}
{# ordered by `date_col` within `partition_by`. The anchor is the first close #}
{# inside `[date_col - window_days, date_col]`, so cross-asset windows align by #}
{# wall-clock time even when assets trade on different calendars (FX vs crypto #}
{# vs equities). Adapter-neutral: `RANGE BETWEEN INTERVAL` is supported by both #}
{# DuckDB and ClickHouse on date columns. #}

{% macro rolling_return(close_col, date_col, partition_by, window_days) %}
    (
        {{ close_col }} / NULLIF(
            FIRST_VALUE({{ close_col }}) OVER (
                PARTITION BY {{ partition_by }} ORDER BY {{ date_col }}
                RANGE BETWEEN INTERVAL '{{ window_days }} days' PRECEDING AND CURRENT ROW
            ),
            0
        ) - 1.0
    )
{% endmacro %}
