CREATE SCHEMA comp;
CREATE EXTENSION jsonbd SCHEMA comp;
CREATE TABLE comp.t(a SERIAL, b JSONB COMPRESSION jsonbd);
\d+ comp.t;

CREATE OR REPLACE FUNCTION comp.add_record()
RETURNS VOID AS $$
BEGIN
	INSERT INTO comp.t(b)
		SELECT jsonb_object(array_agg(array[repeat(letter, count), count::text]))
	FROM (
		SELECT chr(i) AS letter, b AS count
		FROM generate_series(ascii('a'), ascii('z')) i
		FULL OUTER JOIN
		(SELECT b FROM generate_series(10, 20) b) t2
		ON 1=1
	) t3;
END
$$ LANGUAGE plpgsql;

SELECT comp.add_record();
SELECT comp.add_record();
SELECT comp.add_record();

SELECT * FROM comp.t ORDER BY a;
SELECT * FROM comp.t ORDER BY a;

DROP SCHEMA comp CASCADE;
