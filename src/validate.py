from pyspark.sql.functions import col, countDistinct, sum

def validate(df):
    total_rows = df.count()
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])

    # Find columns where null count = total rows
    completely_null = [c for c in df.columns if null_counts.collect()[0][c] == total_rows]

    # Check that the total amount of rows is not 0
    if total_rows > 0:
        print("PASS: Total amount rows not 0.")
    else:
        print("FAIL: No rows found.")

    # Check that no column is completely NULL
    if not completely_null:
        print("PASS: No column completely NULL")
    else:
        print("FAIL: Existing NULL column.")

    unique_count = df.select(countDistinct("pickup_hour")).collect()[0][0]

    # Check that there are exactly 24 different pick up hours
    if unique_count == 24:
        print("PASS: Exactly 24 unique pick up hours.")
    else:
        print("FAIL: Not 24 unique pick up hours.")

    