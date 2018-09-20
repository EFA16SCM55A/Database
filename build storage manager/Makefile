CC=gcc
CFLAGS=-I.
DEPS = dberror.h testhelper.h storage_mgr.h test_helper.h
OBJ = test_assign1_1.o storage_mgr.o dberror.o
%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test_assign1: $(OBJ)
	gcc -o $@ $^ $(CFLAGS)

clean: 
	rm -f *.o test_assign1
