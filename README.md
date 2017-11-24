# jsonbd

JSONB compression method for PostgreSQL.

## Usage

To use it the following patch should be applied to PostgreSQL code (git:master):

https://commitfest.postgresql.org/15/1294/

And something like this:

```
CREATE EXTENSION jsonbd;
CREATE TABLE t(a JSONB COMPRESSED jsonbd);
```

This extension is in development and not finished yet.
