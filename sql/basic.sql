SELECT array_agg(array[repeat(letter, count), count::text])
FROM (
	SELECT chr(i) AS letter, b AS count
	FROM generate_series(ascii('a'), ascii('z')) i
	FULL OUTER JOIN
		(SELECT b FROM generate_series(1, 10) b) t2
	ON 1=1) t3;
