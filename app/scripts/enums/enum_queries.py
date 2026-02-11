from enum import Enum


class Queries(Enum):
    GROUPBY = 'groupby'
    GROUPBY_FILTER = 'groupby_filter'
    GROUPBY_SORTBY_LIMIT = 'groupby_sortby_limit'
    JOIN = 'join'
    JOIN_FILTER_GROUPBY = 'join_filter_groupby'
    JOIN_THREE_TABLES = 'join_three_tables'
    SCAN_FILTER = 'scan_filter'
    WINDOW_FUNCTION = 'window_function'