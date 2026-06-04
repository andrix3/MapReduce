# C11 MapReduce Framework

Questo progetto implementa un framework **MapReduce** multi-processo e multi-thread scritto in C11. Sviluppato come libreria statica (`libmr.a`), il sistema astrae la complessità della gestione di processi (`fork`), comunicazione inter-processo (`pipe`) e sincronizzazione tra thread (`threads.h`), consentendo all'utente di definire esclusivamente la logica di analisi dei dati tramite le funzioni `map` e `reduce`.

Il framework è pienamente conforme alle specifiche del corso Laboratorio 2 A (a.a. 2025-26).

---

## Architettura del Sistema

Il framework adotta un'architettura a **pipeline** composta da tre livelli di elaborazione distinti:

1.  **Processo Main**: Responsabile della scansione del file system (file singoli o directory), della lettura dell'input riga per riga e dell'alimentazione della pipeline. Gestisce inoltre la collezione dei risultati finali e la scrittura ordinata sul file di output.
2.  **Processo Mapper**: Riceve le righe dal Main. Al suo interno, un thread lettore alimenta una coda sincronizzata da cui attingono molteplici **thread worker** (configurabili). Ogni worker invoca la funzione `mapper` dell'utente ed emette coppie `<token, valore>` verso il Reducer.
3.  **Processo Reducer**: Riceve le coppie dal Mapper. Raccoglie tutti i dati in memoria, esegue un raggruppamento deterministico basato sul token e distribuisce i gruppi a un pool di **thread worker**. Ogni gruppo (un token e tutti i suoi valori associati) viene elaborato da un'unica invocazione della funzione `reducer`.

---

## Dettagli Tecnici e Protocolli

### Protocollo di Comunicazione (IPC)
La comunicazione sulle pipe POSIX avviene tramite un protocollo robusto basato su **lunghezze esplicite** (header `int`), garantendo l'integrità di dati binari opachi. Ogni messaggio ha il formato:
`[int token_len] [int data_len] [token bytes] [data bytes]`

### Formato del File di Output
Il framework produce un file binario deterministico in cui i risultati sono ordinati lessicograficamente per token. Ogni record segue la specifica:
`[int token_len] [token bytes] [int result_len] [result bytes]`
*(Nota: token_len non include il terminatore '\0')*

### Sistema di Logging
Il log di esecuzione (`mr.log` di default) è sincronizzato sia a livello di thread (mutex) che di processo (`fcntl` locks). Il formato di ogni riga è:
`[timestamp] [PID] [TID] [evento] messaggio`
Eventi tracciati: creazione pipe/processi, start/end thread, I/O file, statistiche Mapper/Reducer ed errori.

---

## Struttura del Progetto

- **`include/mr.h`**: Interfaccia pubblica obbligatoria.
- **`src/mr.c`**: Implementazione core del framework e coordinamento pipeline.
- **`src/utils/`**: Utility per I/O robusto (`readn`/`writen`), gestione errori e motore di logging.
- **`examples/`**: Esempi d'uso, tra cui `wordcount.c` e l'utility `mr_viewer.c` per l'ispezione dei file binari.
- **`tests/test_suite.c`**: Batteria di test automatizzati (funzionali, binari e stress test).

---

## Compilazione ed Utilizzo

Il progetto utilizza un `Makefile` conforme alle richieste:

```bash
make          # Compila la libreria libmr.a, l'esempio wordcount e il viewer
make test     # Esegue la suite di test automatizzata
make clean    # Rimuove tutti i file generati (oggetti, librerie, eseguibili e log)
```

### Esempio di Esecuzione
Per eseguire l'analisi del conteggio parole su una directory di test:
1. `./examples/wordcount examples/input_test output.mro`
2. `./examples/mr_viewer output.mro` (per visualizzare i risultati binari a schermo)

---

## Note sulla Documentazione
In conformità con la Sezione 16 di `testo.txt`, i dettagli implementativi approfonditi (strutture dati di raggruppamento, algoritmi di sincronizzazione e gestione dei segnali) sono documentati nella **Relazione Tecnica (PDF)** allegata al progetto.
