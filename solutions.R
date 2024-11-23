## Make sure you have the {duckdb} and {dplyr} packages installed.

library(duckdb)
library(dplyr)

## The data dictionary for the dataset is available at:
## https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2021.txt


## Data set exploration with DuckDB

con <- dbConnect(duckdb(), ":memory:")

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>% 
  head()

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>% 
  colnames()

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>% 
  tail()

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>% 
  count()

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>% 
  distinct(ST)

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>% 
  distinct(ST, year)

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>% 
  count(ST, year)

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>% 
  count(ST, year) %>% 
  arrange(ST, year) %>% 
  collect()

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>%
  summarize(n = sum(PWGTP, na.rm = TRUE), .by = c(ST, year)) %>% 
  arrange(ST, year) %>% 
  collect()

## Practice:
##
## What is the mean age per year and per state?
## The age of the respondents is in the column AGEP.
## Don't forget to use the PWGTP column to take the weight
## of each row into account

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>%
  summarize(n = sum(AGEP * PWGTP) / sum(PWGTP), .by = c(ST, year)) %>%
  arrange(ST, year) %>%
  collect()

## Practice:
##
## What is the proportion of children (less than 18 years old),
## adult (between 18 and 69 years old), and elderly in each state?
## Use case_when() to recode the age column (AGEP)

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>%
  mutate(age_group = case_when(
    AGEP < 18 ~ "child",
    AGEP >= 70 ~ "elderly",
    TRUE ~ "adult"
  )) %>%
  summarize(n = sum(PWGTP), .by = c(ST, year, age_group)) %>%
  mutate(p = n / sum(n), .by = c(ST, year)) %>%
  arrange(ST, year) %>%
  collect()

## Practice:
##
## What is the proportion of people who worked from home
## in 2021 in each state?
## Use the column JWTRNS to find the respondent who worked
## from home.

tbl(con, "read_parquet('data/pums_person_sample.parquet')") %>%
  filter(!is.na(JWTRNS), year == 2021) %>%
  summarize(n = sum(PWGTP), .by = c(ST, JWTRNS)) %>%
  mutate(p = n / sum(n), .by = ST) %>%
  filter(JWTRNS == "Worked from home") %>%
  collect()

dbDisconnect(con)

## Using DuckDB files to save tables
## 
## everything up to this point, very exploratory. we didn't save anything. Need
## to recalculate if doing it again. And we read from the same file every time.
## If the file was much larger, that would be inefficient.

con <- dbConnect(duckdb(), dbdir = "pums-small.duckdb", read_only = FALSE)

## Create the table using SQL. This approach scales to very large datasets.
dbExecute(
  con,
  "CREATE TABLE pums_person_small AS SELECT * FROM read_parquet('data/pums_person_sample.parquet');"
  )
dbListTables(con)

## Create the mean_commute_time table
## Here, we also show another way of creating a table within the database
## using the dbWriteTable() function.
mean_commute_time <- tbl(con, "pums_person_small") %>% 
  summarize(mean_commute_time = sum(JWMNP * PWGTP) / sum(PWGTP), .by = c(ST, year)) %>% 
  collect()

dbWriteTable(con, "mean_commute_time", mean_commute_time)
dbListTables(con)

tbl(con, "mean_commute_time")
dbDisconnect(con)
rm(con)

## More advanced data manipulation

con <- dbConnect(duckdb(), "pums-small.duckdb")
dbListTables(con)

## Using DuckDB functions in mutate()
tbl(con, "pums_person_small") %>% 
  left_join(tbl(con, "mean_commute_time"), by = c("ST", "year")) %>% 
  select(ST, year, JWMNP, PWGTP, mean_commute_time, OCCP) %>%
  filter(!is.na(JWMNP)) %>% 
  mutate(diff_commute = JWMNP - mean_commute_time) %>%
  # mutate(category = stringr::str_split_i(OCCP, "-", 1))
  mutate(category = sql("split_part(OCCP, '-', 1)"))

## Using pure SQL
dbGetQuery(
  con,
  "
  SELECT ST, year, DRIVESP, PWGTP, split_part(DRIVESP, ' ', 1)::FLOAT AS carpool
  FROM pums_person_small
  WHERE DRIVESP NOT NULL
  "
)

dbExecute(
  con,
  "
  CREATE TABLE carpool AS (
  SELECT ST, year, DRIVESP, PWGTP, split_part(DRIVESP, ' ', 1)::FLOAT AS carpool
  FROM pums_person_small
  WHERE DRIVESP NOT NULL)
  "
)

tbl(con, "carpool") %>% 
  summarize(cpi = sum(PWGTP * carpool) / sum(PWGTP), .by = c("ST", "year")) %>% 
  arrange(ST, year) %>% 
  collect()

## Practice
##
## The JWAP column contains the time of arrival at work. The data is reported
## as a text string such as '8:00 a.m. to 8:04 a.m.'. We want to extract the 
## the first time reported in this column (here 8.00 a.m.) and put it in a
## a column where it is correctly stored as a time data type.
## Hint: we'll use the replace(), trim(), split_part(), and strptime() functions
## from DuckDB to extract the time. The format specifier for strptime for the
## time is '%I:%M %p'.
##
## Note: contrary to {dplyr}, you can't reuse a mutated column in a single
## mutate() call. You need to call the mutate() function multiple times.

tbl(con, "pums_person_small") %>%
  select(ST, year, JWAP) %>%
  filter(!is.na(JWAP)) %>%
  mutate(time_start = sql("replace(JWAP, '.', '')")) %>%
  mutate(
    time_start = sql("strptime(
    trim(split_part(time_start, 'to', 1)),
    '%I:%M %p'
  )::TIME"))

## There was a question about the slice function.
##
## `slice()` is not implemented for databases, because
## the ordering of the data is not deterministic.
## But other functions in the slice family are available.
## For instance, `slice_sample()` will select random rows
## from the table. And `slice_min()` and `slice_max()`,
## select rows with the smallest and largest values of a 
## variable.

## Example: random sample of 10 rows
tbl(con, "pums_person_small") %>%
  slice_sample(n = 10)

## Example: maximum commute time for each ST/year
tbl(con, "pums_person_small") %>% 
  select(ST, year, JWMNP) %>% 
  slice_max(JWMNP, n = 1, by = c(ST, year), with_ties = FALSE)

## There was a question about type changes using `mutate()`

## Some conversions are supported. For instance, taking the
## previous exercise, we can convert the column to character,
## (as below), but other conversions might not be supported.

tbl(con, "pums_person_small") %>%
  select(ST, year, JWAP) %>%
  filter(!is.na(JWAP)) %>%
  mutate(time_start = sql("replace(JWAP, '.', '')")) %>%
  mutate(
    time_start = sql("strptime(
    trim(split_part(time_start, 'to', 1)),
    '%I:%M %p'
  )")) %>% 
  mutate(time_start = as.character(time_start))
