# contrib/jsonbd/Makefile

MODULE_big = jsonbd
OBJS= jsonbd.o jsonbd_worker.o jsonbd_utils.o $(WIN32RES)

EXTENSION = jsonbd
DATA = jsonbd--0.1.sql
PGFILEDESC = "jsonbd - jsonb compression method"

REGRESS = basic

ifndef PG_CONFIG
PG_CONFIG = pg_config
endif
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

python_tests:
	${MAKE} -C tests python_tests
