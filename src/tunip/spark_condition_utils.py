
def check_valid_string_column(column_name):
    return f"({column_name} is not null) and ({column_name} != '')"
