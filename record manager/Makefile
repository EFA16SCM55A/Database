CC=gcc
CFLAGS=-I.
DEPS = dberror.h buffer_mgr_stat.h storage_mgr.h buffer_mgr.h test_helper.h dt.h record_mgr.h expr.h
OBJ = test_assign3_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o record_mgr.o rm_serializer.o expr.o
%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

record_mgr: $(OBJ)
	gcc -o $@ $^ $(CFLAGS)

clean: 
	rm -f *.o record_mgr

