# Exploring Parking Violations in New York City Using Python and Spark

### Task 1
Write a Spark program that finds all parking violations that have been paid, i.e., that do not occur in openviolations.csv.

**Output:** A key-value pair per line, where

    key = summons_number
    values = plate_id, violation_precinct, violation_code, issue_date
    
You should separate the key and value by a tab character (‘\t’) and elements within the key/value should
be separated by a comma and a space. Your output format should conform to the following example:

    4617117696 GRV2608, 0, 36, 2016-03-09
    4617863450 HAM2650, 0, 36, 2016-03-24


### Task 2
Write a Spark program that finds the distribution of the violation types, i.e., for each violation code, the
number of violations that have this code.

**Output:** A key-value pair per line, where

    key = violation_code
    value = number of violations
    
You should separate the key and value by a tab character (‘\t’) and elements within the key/value should
be separated by a comma and a space. Your output format should conform to the following example:

    1 159
    2 5


### Task 3
Write a Spark program that finds the total and average amount due in open violations for each license
type.

**Output:** A key-value pair per line, where

    key = license_type
    value = total, average
where total and average are rounded to 2 decimal places.
You should separate the key and value by a tab character (‘\t’) and elements within the key/value should
be separated by a comma and a space. Your output format should conform to the following example:

    PAS 9482469.38, 35.82
    USC 250.00, 125.00

### Task 4
Write a Spark program that computes the total number of violations for vehicles registered in the state of
NY and all other vehicles.

**Output:** 2 key-value pairs with one key-value pair per line.
You should separate the key and value by a tab character (‘\t’). Your output format should conform to the
following example:

    NY 12345
    Other 6789

### Task 5
Write a Spark program that finds the vehicle that has had the greatest number of violations (assume that
plate_id and registration_state uniquely identify a vehicle).

**Output:** One key-value pair
You should separate the key and value by a tab character (‘\t’) and elements within the key/value should
be separated by a comma and a space. Your output format should conform to the following example:

    AP501F, NJ 138

### Task 6
Write a Spark program that finds the top-20 vehicles in terms of total violations (assume that plate id and
registration state uniquely identify a vehicle).

**Output:** List of 20 key-value pairs, ordered by decreasing number of violations.
You should separate the key and value by a tab character (‘\t’) and elements within the key/value should
be separated by a comma and a space. Your output format should conform to the following example:

    AP501F, NJ 138

### Task 7
In March 2016, the 5th, 6th, 12th, 13th, 19th, 20th, 26th, and 27th were weekend days (i.e., Sat. and
Sun.).
Write a Spark program that, for each violation code, lists the average number of violations with that code
issued per day on weekdays and weekend days. You may hardcode “8” as the number of weekend days
and “23” as the number of weekdays in March 2016.

**Output:** List of key-value pairs where

    key = violation_code
    value = weekend_average, week_average
    
where weekend_average and week_average are rounded to 2 decimal places.
You should separate the key and value by a tab character (‘\t’) and elements within the key/value should
be separated by a comma and a space. Your output format should conform to the following example:

    1 3.25, 5.78
    2 0.12, 0.17
