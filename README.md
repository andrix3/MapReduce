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

Alla luce delle specifiche contenute in Testo.pdf, questa è la TODO list aggiornata per completare il progetto in accordo con i requisiti:

1. **Gestione del Logging Condiviso**:
   - Implementare la scrittura su file di log (es: `mr.log`) rispettando ESATTAMENTE il formato richiesto: `[timestamp] [processo] [thread] [evento] messaggio`.
   - Sincronizzare gli accessi al log (es. tramite semafori POSIX) poiché più processi/thread scriveranno in modo concorrente sullo stesso file.
   - Registrare tutti gli eventi richiesti: creazione pipe e processi, thread C11, chiusura stream, righe lette, token aggregati, ecc.

2. **Protocollo Interno Pipe (Rimozione `size_t` cross-arch)**:
   - Modificare le chiamate sulle syscall `readn` e `writen` in modo che la dimensione dell'header (es. token length o result length) venga gestita tramite tipi `int` (controllando che non siano negativi!) come da PDF, e non `size_t`. 
   - Rimuovere il concetto di un messaggio speciale "EOF". La fine del flusso deve essere gestita esclusivamente intercettando lo _0_ (EOF) della `read` causato da una `close()` del lato di scrittura della file descriptor remota.
   - Non assumerne alcun invio esplicito del `'\0'` al reducer/mapper poiché non fa parte dei field originali di `mr_value_t`.

3. **Logica Processo Mapper (`mapper_process_main`)**:
   - Avviare il thread lettore che incoda righe disgiunte nella bounded-queue interna `mtx_t` / `cnd_t` (grande al più `attr.queue_size`).
   - Lanciare `N` thread worker C11 (da config `mapper_threads`) che leggono riga e lanciano la funzione user `mapper`. 
   - I worker scrivono, in mutua esclusione, il dato struct `pair` formattato nel buffer pipe diretto a Reducer.

4. **Logica Processo Reducer (`reducer_process_main`)**:
   - Implementare Thread lettore da `mapper_to_reducer` reader-side per riempire progressivamente una data-structure di raccoglimento.
   - Strutturare una hash map, un albero o un array con ordinamento per collezionare le values che afferiscono *allo stesso* token.
   - Appena la pipe fa scattare l'EOF (chiusura totale da Mapper), raggruppare i valori e distribuire ai thread reducer worker C11 (`reducer_threads`) in task paralellely i vari bucket `<token, processed_token[]>`.
   - Recuperare le elaborazioni per spararle inter-process al Main verso `reducer_to_main`.

5. **Output Deterministico e Scrittura finale nel file `mr_start`**:
   - Nel Main parent: recuperare i result serializzati e consolidare il tracking.
   - Salvare in output nel file (`output_path`) in logica deterministica: **ordinati lessicograficamente** per token. I record devono contenere esplicitamente lunghezze e bytes senza assumption.

6. **Target Aggiuntivi (Addendum - se richiesto dal docente)**:
   - Implementare scansione ricorsiva (in `process_multiple_files`).
   - Implementare parametrizzazione di una "hash function" custom deterministica (nel framework attributi e chiamata `mr_attr_set_hash_function`).
   - Possibilità di computazioni in overlay (più `mr_create` in isolamento locale).
   - Generazione file separato statistiche di runtime.

7. **Aggiornamento Tests (`make test`)**:
   - Creare un sistema auto-checking su file e singole directory e sui payload critici (EOF improvviso e newline bypassate).

   ---

   ## Pulizia e refactor effettuati

   - Rimosso lo script temporaneo `update_mapper.sh` e lo script di inserimento `insert.py` utilizzati solo per patch manuali.
   - Compilazione verificata con `make`: generati `libmr.a`, `examples/wordcount` e `tests/test_suite` senza errori di compilazione rilevanti.

   Se vuoi, posso eseguire ora l'esempio `examples/wordcount` su una piccola directory di test per verificare l'intera pipeline end-to-end.

