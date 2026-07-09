CC = gcc
CFLAGS = -Wall -Wextra -Werror -Wpedantic -std=c11 -D_DEFAULT_SOURCE -pthread
INCLUDES = -Iinclude -Isrc/utils
LIBS = -lpthread 

LIB_NAME = libmr.a
SRC_FILES = src/mr.c src/utils/utils.c src/utils/log_internal.c
OBJ_FILES = $(SRC_FILES:.c=.o)

EXAMPLE_EXE = examples/wordcount
VIEWER_EXE = examples/mr_viewer
TEST_EXE = tests/test_suite

all: $(LIB_NAME) $(EXAMPLE_EXE) $(VIEWER_EXE) $(TEST_EXE)

$(LIB_NAME): $(OBJ_FILES)
	ar rcs $@ $^

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(EXAMPLE_EXE): examples/wordcount.c $(LIB_NAME)
	$(CC) $(CFLAGS) $(INCLUDES) $< -L. -lmr $(LIBS) -o $@

$(VIEWER_EXE): examples/mr_viewer.c
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

test: $(TEST_EXE)
	./$(TEST_EXE)

$(TEST_EXE): tests/test_suite.c $(LIB_NAME)
	$(CC) $(CFLAGS) $(INCLUDES) $< -L. -lmr $(LIBS) -o $@

clean:
	rm -f $(OBJ_FILES) $(LIB_NAME) $(EXAMPLE_EXE) $(VIEWER_EXE) $(TEST_EXE) mr.log
	rm -f tests/*.o examples/*.o

.PHONY: all test clean
