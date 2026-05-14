#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <threads.h>
#include <sys/wait.h>
#include <sys/stat.h>

#include "mr.h"
#include "error_utils.h"
#include "utils.h"
#include "log_internal.h"

/* --- Strutture Interne --- */

/**
 * Definizione della struttura opaca mr_t.
 * Contiene lo stato necessario per gestire l'intera pipeline.
 */
struct mr
{
    mr_attr_t attr;            // Copia degli attributi di configurazione
    mr_mapper_t user_mapper;   // Funzione mapper dell'utente
    mr_reducer_t user_reducer; // Funzione reducer dell'utente
    void *user_arg;            // Argomento utente da passare alle callback

    pid_t mapper_pid;  // PID del processo figlio mapper
    pid_t reducer_pid; // PID del processo figlio reducer

    // Descrittori delle pipe necessari al processo principale
    int main_to_mapper_write;
    int reducer_to_main_read;
};

/**
 * Header per il protocollo di comunicazione sulle pipe
 * Usato per trasmettere coppie (token, valore) e risultati finali.
 */
typedef struct
{
    int token_len;
    int value_len;
} mr_pair_header_t;

/* --- Prototipi di Funzioni Ausiliarie (Interne) --- */

// Gestione letture e scritture parziali su pipe
static ssize_t readn(int fd, void *buf, size_t n);
static ssize_t writen(int fd, const void *buf, size_t n);

// Funzioni principali eseguite dai processi figli
static void mapper_process_main(struct mr *mr, int pipe_in, int pipe_out);
static void reducer_process_main(struct mr *mr, int pipe_in, int pipe_out);

// Funzioni per i thread (mapper, reducer e lettori)
static int mapper_worker_main(void *arg);
static int reducer_worker_main(void *arg);
static int reader_main(void *arg);

/* --- Implementazione Interfaccia Pubblica --- */

int mr_attr_init(mr_attr_t *attr)
{
    if (!attr)
        return -1;

    attr->mapper_threads = 1;
    attr->reducer_threads = 1;
    attr->queue_size = 16;
    attr->log_file = NULL;

    return 0;
}
int mr_attr_destroy(mr_attr_t *attr)
{
    if (attr == NULL)
        return -1;

    // Non essendoci stati malloc interni in mr_attr_init o nei setter,
    // non c'è nulla da liberare.

    attr->mapper_threads = 0;
    attr->reducer_threads = 0;
    attr->queue_size = 0;
    attr->log_file = NULL;

    return 0;
}

int mr_attr_set_mapper_threads(mr_attr_t *attr, size_t n)
{
    if (!attr || n == 0)
        return -1;

    attr->mapper_threads = n;

    return 0;
}

int mr_attr_set_reducer_threads(mr_attr_t *attr, size_t n)
{
    if (!attr || n == 0)
        return -1;

    attr->reducer_threads = n;

    return 0;
}

int mr_attr_set_queue_size(mr_attr_t *attr, size_t n)
{
    if (!attr || n == 0)
        return -1;

    attr->queue_size = n;

    return 0;
}

int mr_attr_set_log_file(mr_attr_t *attr, const char *path)
{
    if (!attr)
        return -1;

    if (path != NULL)
    {
        struct stat s;
        // Se il percorso esiste, verifichiamo che non sia una directory
        if (stat(path, &s) == 0)
        {
            if (S_ISDIR(s.st_mode))
            {
                errno = EISDIR; // Impostiamo l'errore corretto
                return -1;
            }
        }
        // Nota: se stat fallisce con ENOENT significa che il file non esiste ancora,
        // però potrebbe venir creato in seguito.
    }

    attr->log_file = path; // NULL è valido per il default
    return 0;
}

int mr_create(mr_t *mr, const mr_attr_t *attr, mr_mapper_t mapper, mr_reducer_t reducer, void *user_arg)
{
    if (!mr || !attr || !mapper || !reducer)
        return -1;

    struct mr *new_mr = malloc(sizeof(struct mr));
    if (!new_mr)
        return -1;

    // 1. Copia i valori scalari (mapper_threads, reducer_threads, queue_size)
    new_mr->attr = *attr;

    // 2. Gestione "Deep Copy" del log file
    // Se log_file è NULL, usiamo il default "mr.log"
    const char *target_log = (attr->log_file == NULL) ? "mr.log" : attr->log_file;

    // Usiamo strdup per creare una copia privata della stringa.
    // In questo modo siamo indipendenti dalla memoria dell'utente.
    new_mr->attr.log_file = strdup(target_log);
    if (new_mr->attr.log_file == NULL)
    {
        free(new_mr);
        return -1;
    }

    // 3. Inizializzazione degli altri campi
    new_mr->user_mapper = mapper;
    new_mr->user_reducer = reducer;
    new_mr->user_arg = user_arg;
    new_mr->mapper_pid = -1;
    new_mr->reducer_pid = -1;
    new_mr->main_to_mapper_write = -1;
    new_mr->reducer_to_main_read = -1;

    // Comunichiamo al sistema di log quale file usare effettivamente
    // mr_log_set_path(new_mr->attr.log_file); // NON DEFINITA O RIMOSSA

    *mr = new_mr;
    return 0;
}

int mr_start(mr_t mr, const char *input_path, const char *output_path)
{
    if (!mr || !input_path || !output_path)
        return -1;

    path_type_t input_path_type = check_path(input_path);

    if (input_path_type == PATH_INVALID)
    {
        MR_LOG(mr, "ERROR", "Percorso di input non valido o inaccessibile");
        return -1;
    }

    if (check_output_path(output_path) == -1)
    {
        MR_LOG(mr, "ERROR", "Percorso di output non valido o inaccessibile");
        return -1;
    }

    int pipe_main_to_mapper[2];
    int pipe_mapper_to_reducer[2];
    int pipe_reducer_to_main[2];

    pipe(pipe_main_to_mapper);
    pipe(pipe_mapper_to_reducer);
    pipe(pipe_reducer_to_main);
    
    
    if ((mr->mapper_pid = fork()) == 0)
    {
        // Sono nel processo mapper
        close(pipe_main_to_mapper[1]);
        close(pipe_mapper_to_reducer[0]);
        close(pipe_reducer_to_main[0]);
        close(pipe_reducer_to_main[1]);

        mapper_process_main(mr, pipe_main_to_mapper[0], pipe_mapper_to_reducer[1]);
        
        close(pipe_main_to_mapper[0]);
        close(pipe_mapper_to_reducer[1]);
        
        _exit(EXIT_SUCCESS);
    }
    
    
    if ((mr->reducer_pid = fork()) == 0)
    {
        // Sono nel processo reducer
        close(pipe_main_to_mapper[0]);
        close(pipe_main_to_mapper[1]);
        close(pipe_mapper_to_reducer[1]);
        close(pipe_reducer_to_main[0]);
        
        reducer_process_main(mr, pipe_mapper_to_reducer[0], pipe_reducer_to_main[1]);

        close(pipe_mapper_to_reducer[0]);
        close(pipe_reducer_to_main[1]);

        _exit(EXIT_SUCCESS);
    }
    
    close(pipe_main_to_mapper[0]);
    close(pipe_mapper_to_reducer[0]);
    close(pipe_mapper_to_reducer[1]);
    close(pipe_reducer_to_main[1]);
    
    mr->main_to_mapper_write = pipe_main_to_mapper[1];
    mr->reducer_to_main_read = pipe_reducer_to_main[0];
    
    if (input_path_type == PATH_FILE)
    {
        // Caso semplice: aggiungi il file alla lista dei lavori
        // process_single_file(mr, input_path);
    }
    else if (input_path_type == PATH_DIRECTORY)
    {
        // Caso complesso: usa scandir() per ottenere i file in ordine alfabetico
        // e filtra solo i file regolari (S_ISREG)
    }
    
    close(mr->main_to_mapper_write);
    mr->main_to_mapper_write = -1;

    // ricevo da reducer e scrivo su output

    close(mr->reducer_to_main_read);
    mr->reducer_to_main_read = -1;

    waitpid(mr->mapper_pid, NULL, 0);
    waitpid(mr->reducer_pid, NULL, 0);

    return 0;
}

int mr_destroy(mr_t mr)
{
    if (mr == NULL)
    {
        return -1;
    }

    if (mr->main_to_mapper_write != -1)
    {
        close(mr->main_to_mapper_write);
    }
    if (mr->reducer_to_main_read != -1)
    {
        close(mr->reducer_to_main_read);
    }

    if (mr->attr.log_file) {
        free((char *)mr->attr.log_file);
    }

    // 3. Libera la memoria della struttura principale.
    free(mr);

    return 0;
}

/* --- Implementazione Logica Interna --- */

static void mapper_process_main(struct mr *mr, int pipe_in, int pipe_out)
{
    // Inizializza code produttore-consumatore
    // Avvia thread lettore e thread worker C11
    // Attende terminazione thread e chiude pipe verso il reducer

}

static void reducer_process_main(struct mr *mr, int pipe_in, int pipe_out)
{
    // Legge coppie, raggruppa per token
    // Esegue thread reducer sui gruppi completi
    // Scrive risultati finali sulla pipe verso il main

}