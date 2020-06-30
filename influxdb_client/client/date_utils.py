from dateutil import parser

parse_function = None


def get_date_parse_function():
    global parse_function
    if parse_function is None:
        try:
            import ciso8601
            parse_function = ciso8601.parse_datetime
        except ModuleNotFoundError:
            parse_function = parser.parse

    return parse_function
