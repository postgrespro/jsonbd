# jsonbd

JSONB compression method for PostgreSQL.

## Usage

To use it the following patch should be applied to PostgreSQL code (git:master):

https://commitfest.postgresql.org/15/1294/

And something like this:

```
CREATE EXTENSION jsonbd;
CREATE COMPRESSION METHOD cm1 HANDLER jsonbd_compression_handler;
CREATE TABLE t(a JSONB);
ALTER TABLE t ALTER COLUMN a SET COMPRESSED cm1;
```

This is very early version and should not be used in any real systems.
