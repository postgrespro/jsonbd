CREATE TABLE jsonbc_dictionary(
	cmopt OID NOT NULL,			/* compression options id */
	id INT4	NOT NULL,			/* key id, related to compression options */
	key TEXT NOT NULL			/* jsonb key */
);
CREATE UNIQUE INDEX jsonbc_dict_ids ON jsonbc_dictionary(cmopt, id);
CREATE UNIQUE INDEX jsonbc_dict_keys ON jsonbc_dictionary (cmopt, key);

CREATE OR REPLACE FUNCTION jsonbc_compression_handler(INTERNAL)
RETURNS COMPRESSION_HANDLER AS 'MODULE_PATHNAME', 'jsonbc_compression_handler'
LANGUAGE C STRICT;
