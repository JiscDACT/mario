# Running SQL-based tests

To run unit tests with a SQL database,
you need to 

1. Set up a test database and load
into it the 'orders.csv' dataset. (The tests use 'dev'.'superstore')

2. Add CONNECTION_STRING to your pytest environment variables with the 
connection details for the database