from datetime import datetime
import re

def parse_date_instruction(date_string: str) -> str: 
    """
    The function accepts a string containing instructions for date transformations, which can include operations 
    like addition or subtraction of days, weeks, months, or years to/from a reference date, as well as direct 
    specificationof date literals. These transformations are parsed and converted into expressions suitable for 
    use with PySpark's `f.expr()` function to create new date columns or transform existing ones.
    
    Parameters:
        date_string (str): String containing date instructions.

    Returns:
        str: Parsed expression suitable for use with f.expr().

    Examples:
        >>> parse_date_instruction('2020-01-01')
        "date('2020-01-01')"
        >>> parse_date_instruction('2020-02-30')
        Traceback (most recent call last):
            ...
        ValueError: Invalid date: '2020-02-30'
        >>> parse_date_instruction('index_date + 5 days')
        "index_date + cast(round(5*1) as int)"
        >>> parse_date_instruction('x - 6 weeks')
        "x - cast(round(6*7) as int)"
        >>> parse_date_instruction('index_date + 3 months')
        "index_date + cast(round(3*30) as int)"
        >>> parse_date_instruction('index_date - 2 years')
        "index_date - cast(round(2*365.25) as int)"
        >>> parse_date_instruction('index_date')
        'index_date'
        >>> parse_date_instruction('current_date() + 5 days')
        'current_date() + cast(round(5*1) as int)'
    """

    # Check if date_string is None, if so return 'NULL'
    if date_string is None:
        return 'NULL'

    # Check if the instruction is a simple date string
    elif re.match(r'\d{4}-\d{2}-\d{2}', date_string):
        if validate_date_string(date_string):
            return f"date('{date_string}')"
        else:
            raise ValueError(f"Invalid date: {date_string}")

    # Check if the instruction is a transformation expression
    elif any(unit in date_string for unit in ['day', 'days', 'week', 'weeks', 'month', 'months', 'year', 'years']):
        parsed_expression = convert_date_units_to_days(date_string)
        return parsed_expression

    # Otherwise return original date expression
    else:
        return date_string



def convert_date_units_to_days(date_expression: str) -> str:
    """
    Extracts the number and unit from the date_expression, converts the unit to days
    by multiplying with the appropriate factor (1 for day, 7 for week, 30 for month, 365.25 for year),
    and wraps the expression within the round() function. The result is cast to an integer.

    Valid formats for the date_expression include expressions like 'index_date + 6 months',
    'x - 7.5 weeks', where the number can be an integer or a floating-point number,
    and the unit can be 'day(s)', 'week(s)', 'month(s)', or 'year(s)'.

    Parameters:
        date_expression (str): Date or transformation expression.

    Returns:
        str: A string representing the converted expression with the result cast to an integer.

    Example:
        >>> converted_expression = convert_date_units_to_days('index_date - 2 years, x - 7.5 weeks')
        >>> print(converted_expression)
        'index_date + cast(round(6*30) as int), x - cast(round(7.5*7) as int)'
    """

    pattern = r'\b(\d+(?:\.\d+)?)\s*(day|week|month|year)s?\b'
    matches = re.findall(pattern, date_expression)

    for number, unit in matches:
        if unit in ['day', 'days']:
            converted_unit = '*1'
        elif unit in ['week', 'weeks']:
            converted_unit = '*7'
        elif unit in ['month', 'months']:
            converted_unit = '*30'
        elif unit in ['year', 'years']:
            converted_unit = '*365.25'
        else:
            raise ValueError("Invalid unit. Use 'day', 'week', 'month', or 'year'.")

        converted_expression = f"cast(round({number}{converted_unit}) as int)"
        date_expression = re.sub(rf'\b{number}\s*{unit}s?\b', converted_expression, date_expression)

    return date_expression



def validate_date_string(date_string: str) -> bool:
    """
    Checks if the given date_string is a valid date in the 'YYYY-MM-DD' format.

    The function validates whether the provided date_string represents a valid date in the standard 'YYYY-MM-DD' format.
    It checks if the date string conforms to the specified format and if the date itself exists in the calendar.

    Parameters:
        date_string (str): The date string to validate.

    Returns:
        bool: True if the date_string is a valid date in the 'YYYY-MM-DD' format, False otherwise.

    Examples:
        >>> validate_date_string('2020-01-01')
        True
        >>> validate_date_string('2020-02-30')
        False
    """
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False
