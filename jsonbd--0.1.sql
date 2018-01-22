CREATE OR REPLACE FUNCTION jsonbd_compression_handler(INTERNAL)
RETURNS COMPRESSION_AM_HANDLER AS 'MODULE_PATHNAME', 'jsonbd_compression_handler'
LANGUAGE C STRICT;

CREATE TABLE jsonbd_dictionary(
	acoid	OID NOT NULL,
	id		INT4 NOT NULL,
	key		TEXT NOT NULL
);

CREATE UNIQUE INDEX jsonbd_dict_on_id ON jsonbd_dictionary(acoid, id);
CREATE UNIQUE INDEX jsonbd_dict_on_key ON jsonbd_dictionary(acoid, key);

CREATE ACCESS METHOD jsonbd
	TYPE COMPRESSION HANDLER jsonbd_compression_handler;
