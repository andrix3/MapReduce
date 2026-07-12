# C11 MapReduce Framework

Questo progetto implementa un framework **MapReduce** multi-processo e multi-thread scritto in C11. È stato sviluppato per il progetto del corso di Laboratorio 2 A (a.a. 2025-26) e viene compilato come libreria statica (`libmr.a`). Il framework permette all'utente di definire solo la logica di analisi dei dati (tramite le funzioni `map` e `reduce`), occupandosi automaticamente della gestione dei processi (`fork`), della comunicazione (`pipe`) e della sincronizzazione (`<threads.h>`).

---

## Architettura del Sistema

Il framework usa un'architettura a tre processi disposti in pipeline:

1.  **Processo Principale (Main)**: Si occupa di scansionare i file o le directory in input, leggere il contenuto riga per riga e passarlo al Mapper. Alla fine, raccoglie i risultati elaborati e li salva ordinatamente sul file di output.
2.  **Processo Mapper**: Riceve le righe dal Main. Un thread dedicato alla lettura le mette in una coda protetta, da cui un pool di thread worker estrae i messaggi, esegue la funzione `mapper` dell'utente e invia le coppie `<token, valore>` prodotte verso il Reducer.
3.  **Processo Reducer**: Riceve le coppie intermedie dal Mapper e le accumula in memoria. Alla fine del flusso, raggruppa tutti i valori associati allo stesso token e assegna i vari gruppi ai suoi thread worker per eseguire la funzione `reducer` e generare i risultati finali.

---

## Dettagli Tecnici

### Protocollo di Comunicazione sulle Pipe
Per evitare la perdita o la corruzione di dati binari (che possono contenere anche `\0`), la comunicazione tra i processi non usa delimitatori testuali, ma un protocollo custom basato sull'invio preventivo delle lunghezze. Ogni messaggio è così strutturato:
`[int token_len] [int data_len] [byte del token] [byte del payload]`

### Formato del File di Output
Il framework genera un file binario deterministico, con i record ordinati alfabeticamente in base al token:
`[int token_len] [byte del token] [int result_len] [byte del risultato]`
*(Nota: la lunghezza `token_len` non include il terminatore '\0').*

### Sistema di Logging
Il framework salva un registro degli eventi (di default su `mr.log`), gestendo la concorrenza con mutex per i thread e con lock `fcntl` per i processi. Ogni riga ha il formato:
`[timestamp] [PID] [evento] messaggio`
Vengono tracciati eventi come la creazione delle pipe, l'avvio dei processi e dei thread, gli I/O sui file ed eventuali errori.

---

## Struttura del Progetto

- **`include/mr.h`**: L'interfaccia pubblica del framework.
- **`src/mr.c`**: Implementazione della logica MapReduce e gestione pipeline.
- **`src/utils/`**: Utility per l'I/O sicuro (`readn`/`writen`) e gestione del logging interno.
- **`examples/`**: Esempi pratici, tra cui il `wordcount` e l'utility `mr_viewer` per ispezionare il contenuto dei file binari.
- **`tests/test_suite.c`**: Batteria di test per collaudare il framework anche nei casi limite.

---

## Compilazione ed Esecuzione

Il progetto include un `Makefile` già configurato:

```bash
make          # Compila la libreria libmr.a, l'esempio wordcount e il viewer
make test     # Compila ed esegue automaticamente la suite di test
make clean    # Pulisce la directory eliminando file oggetto, librerie, log e binari
```
## Esempio di utilizzo
Per provare il framework con l'esempio del conteggio parole(dopo aver compilato):
```bash
./examples/wordcount examples/input_test output.mro   # Per generare output
./examples/mr_viewer output.mro                       # Per visualizzare output
``` 
## Relazione
Per maggiori dettagli sulle scelte implementative, la sincronizzazione dei thread C11 e la gestione della memoria, si faccia riferimento alla relazione in formato PDF allegata al progetto.
