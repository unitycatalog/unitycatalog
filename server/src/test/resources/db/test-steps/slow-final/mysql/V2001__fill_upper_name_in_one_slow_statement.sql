-- Fills every row's upper-case name in one statement that takes 5 seconds (1 per row).
UPDATE test_data_step_items SET upper_name = UPPER(name) WHERE SLEEP(1) = 0;
