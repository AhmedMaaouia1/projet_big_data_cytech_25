from datetime import date


def iter_months_inclusive(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
):
    if (end_year, end_month) < (start_year, start_month):
        raise ValueError("End date must be after start date")

    year, month = start_year, start_month
    months = []

    while (year, month) <= (end_year, end_month):
        months.append(date(year, month, 1))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    return months
