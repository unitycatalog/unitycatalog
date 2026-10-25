-- Fills every row's upper-case name in one statement that takes 5 seconds.
UPDATE test_data_step_items SET upper_name = UPPER(name) WHERE (SELECT 1 FROM pg_sleep(5)) = 1;
