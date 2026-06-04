# Variabili di compilazione
CC = gcc
CFLAGS = -Wall -Wextra -Wpedantic -std=c11 -D_DEFAULT_SOURCE -pthread
INCLUDES = -Iinclude -Isrc/utils
LIBS = -lpthread 

# Target della libreria 
LIB_NAME = libmr.a
LIB_DIR = .

# File sorgente del framework
SRC_FILES = src/mr.c \
            src/utils/utils.c \
            src/utils/log_internal.c
OBJ_FILES = $(SRC_FILES:.c=.o)

# Esempio, Utility e Test
EXAMPLE_SRC = examples/wordcount.c
EXAMPLE_EXE = examples/wordcount
VIEWER_SRC = examples/mr_viewer.c
VIEWER_EXE = examples/mr_viewer
TEST_SRC = tests/test_suite.c
TEST_EXE = tests/test_suite

# --- Target Principali ---

# Target di default: compila tutto 
all: $(LIB_NAME) $(EXAMPLE_EXE) $(VIEWER_EXE) $(TEST_EXE)

# Creazione della libreria statica 
$(LIB_NAME): $(OBJ_FILES)
	ar rcs $@ $^

# Regola generica per i file oggetto
%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# Compilazione dell'esempio (linka la libreria appena creata) 
$(EXAMPLE_EXE): $(EXAMPLE_SRC) $(LIB_NAME)
	$(CC) $(CFLAGS) $(INCLUDES) $< -L. -lmr $(LIBS) -o $@

# Compilazione dell'utility viewer
$(VIEWER_EXE): $(VIEWER_SRC)
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

# Target per i test automatici
test: $(TEST_EXE)
	./$(TEST_EXE)

$(TEST_EXE): $(TEST_SRC) $(LIB_NAME)
	$(CC) $(CFLAGS) $(INCLUDES) $< -L. -lmr $(LIBS) -o $@

# Pulizia dei file generati 
clean:
	rm -f $(OBJ_FILES) $(LIB_NAME) $(EXAMPLE_EXE) $(VIEWER_EXE) $(TEST_EXE) mr.log
	rm -f tests/*.o examples/*.o

.PHONY: all test clean
