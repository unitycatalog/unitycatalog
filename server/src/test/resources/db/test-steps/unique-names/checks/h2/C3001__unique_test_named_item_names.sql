SELECT id, name FROM test_named_items
WHERE name IN (SELECT name FROM test_named_items GROUP BY name HAVING COUNT(*) > 1)
ORDER BY id;
