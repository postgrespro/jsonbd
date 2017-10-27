# contrib/jsonbc/Makefile

MODULE_big = jsonbc
OBJS= jsonbc.o $(WIN32RES)

EXTENSION = jsonbc
DATA = jsonbc--0.1.sql
PGFILEDESC = "jsonbc - jsonb compression method"

REGRESS = basic

ifdef USE_PGXS
ifndef PG_CONFIG
PG_CONFIG = pg_config
endif
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/jsonbc
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
