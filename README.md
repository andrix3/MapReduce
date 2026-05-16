# C11 MapReduce Framework

Questo progetto implementa uno scheletro per un framework **MapReduce** multi-processo e multi-thread scritto interamente in C11. Il progetto è concepito per produrre una libreria statica (`libmr.a`) capace di astrarre all'operatore la complessità della concorrenza (fork, pipe, thread) fornendo interfacce semplici per la programmazione MapReduce su singoli file testuali o intere directory.

## Struttura del Progetto

Il codice sorgente è organizzato per mantenere netta la separazione tra API pubblica, core map-reduce, utility POSIX ed esempi:

- **`include/`**
  - `mr.h`: Header pubblico. Espone i tipi (`mr_t`, `mr_attr_t`), le firme da implementare per `mapper` e `reducer`, e le interfacce (`mr_create`, `mr_start`, ecc.)
- **`src/`**: Motore della libreria MapReduce.
  - `mr.c`: Nucleo vero e proprio. Si occupa di scansionare i file usando i thread, lanciare i processi secondari concorrenti (Mapper e Reducer via `fork()`) ed agganciarne la comunicazione su apposite pipe.
  - **`utils/`**: Moduli ausiliari scorporati dalla logica primaria.
    - `log_internal.c` / `.h`: Gestione automatica o manuale dei log su file o su standard output.
    - `utils.c` / `.h`: Routine ausiliarie per il check del filesystem POSIX (controlli R/W ok, file/dir) e letture/scritture robuste (`readn`/`writen`) adatte per I/O asincrono su descrittori di rete o pipe inter-processo.
    - `error_utils.h`: Macro di utilità per il check formale delle syscall (costrutti fail-fast).
- **`examples/`**
  - `wordcount.c`: Esempio di programma target dell'utente finale. Utilizza le interfacce del framework per analizzare le parole in file text-based.
- **`tests/`**
  - `test_suite.c`: Set di base per confermare la stabilità della libreria.

## Build ed Esecuzione

Il progetto ha a disposizione un Makefile standard per compilazione e pulizia. Include automaticamente la corretta flag `-pthread` richiesta per i mutex ed i worker thread. Si lancia posizionandosi nella root di directory `/mapreduce/`.

```bash
make          # Genera in \'.\' libmr.a, examples/wordcount e tests/test_suite
make clean    # Rimozione dei file oggetto e dei binari e del file temporaneo mr.log
```

## Strumenti e API (Come si usa)

L'utilizzo del framework MapReduce tramite l'API copre generalmente quattro macro-fasi logiche nel tuo driver program:
1. Configurazione: Inizializzare `mr_attr_t` e settare pool thread e files log (`mr_attr_init`, `mr_attr_set_mapper_threads`...).
2. Bootstrap: Creare l'oggetto driver (`mr_t`) iniettando le funzioni che mapperanno i dati.
3. Start: Richiedere processamento da input-directory su output-directory `mr_start(mr, "/tuo/input", "/tuo/output");`.
4. Tear-Down: Rilasciare i descrittori e liberare gli handle `mr_destroy()`.

---

# Prossimi step 

1. Rendere coerenti logging + error utils
2. implementare mr_log_internal() (almeno append su file con timestamp + pid/tid).
3. sostituire mr_log_error_internal con mr_log_internal o aggiungere wrapper reale.
4. Definire protocollo pipe
5. header unico per messaggio + tipo messaggio (LINE, PAIR, RESULT, EOF).
6. Implementare mapper_process_main
7. reader thread legge LINE da pipe main
queue bounded (attr.queue_size)
8. N worker thread chiamano user_mapper e scrivono PAIR su pipe verso reducer
9. Implementare reducer_process_main
10. accumulo per token (hash map) + quando finito input, esegue reduce e invia RESULT
11. Implementare output su mr_start
12. scrivere su output_path (chiarire se directory o file unico).
13. Test veri
14. un test che lancia wordcount su input piccolo e verifica output.
