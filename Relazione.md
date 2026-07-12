# Relazione Progetto: Framework MapReduce (libmr)

Studente: Andrea Del Carlo

Matricola: 692344

Corso: Laboratorio 2 A (a.a. 2025-26)

## Architettura generale del framework

Il framework libmr viene avviato dal programma utente tramite la funzione mr_start(). L'obiettivo è implementare il paradigma MapReduce in locale su un singolo calcolatore.

All'inizio, il sistema controlla la validità dei percorsi di input e output. Successivamente, il processo principale crea tre pipe e fa due fork() per generare i processi mapper e reducer.

A questo punto i dati scorrono in pipeline: il processo principale legge le righe dal file e le manda via pipe al mapper. Il mapper le inserisce in una coda gestita da vari thread worker, i quali chiamano la funzione utente per mappare e mandano i token intermedi al reducer sulla seconda pipe. Il reducer riceve tutto, raggruppa i dati con lo stesso token ordinandoli, e i suoi thread worker richiamano la funzione di riduzione dell'utente. Infine, i risultati arrivano al processo principale sull'ultima pipe per essere scritti sul file binario di destinazione in modo ordinato.

## Interfaccia pubblica

Tutta l'API per l'utente è definita nel file include/mr.h. Ho scelto di utilizzare il tipo opaco mr_t (un puntatore a una struct nascosta) per garantire l'information hiding: in questo modo il chiamante non può toccare o corrompere lo stato interno, come i PID o i file descriptor.

Per la configurazione prima dell'avvio, l'utente utilizza la struct pubblica mr_attr_t. Qui può impostare il numero di thread per il mapper e per il reducer, la dimensione delle code (queue_size) e il file di log. Le funzioni mr_attr_set_* controllano che i parametri siano validi e rifiutano configurazioni scorrette (come richiedere 0 thread).

## Organizzazione dei processi

Ho diviso l'architettura su 3 processi connessi tramite pipe (p1, p2, p3).

Il primo figlio generato è il mapper: tramite dup2() collego il suo stdin in lettura alla pipe p1 e il suo stdout in scrittura alla pipe p2.

Il secondo figlio è il reducer, che legge da p2 e scrive su p3 in direzione del padre.

Un punto fondamentale di questa implementazione è la corretta chiusura dei file descriptor ereditati dopo le fork. Ogni processo chiude tempestivamente tutto ciò che non gli serve. Questo è l'unico modo per far funzionare correttamente l'EOF: ad esempio, quando il main finisce di leggere il file, chiude la scrittura su p1; questo fa arrivare un EOF al mapper, che una volta finito il suo lavoro chiuderà p2, propagando a cascata la chiusura fino al reducer e al main, evitando deadlock.

## Organizzazione dei thread C11

Ho usato i thread C11 (<threads.h>) all'interno dei processi figli.

Nel mapper, c'è un singolo thread dedicato solo a leggere dalla pipe (per non bloccare le altre operazioni) che inserisce i messaggi in una coda, e un pool di thread "worker" che estraggono le righe dalla coda in parallelo e applicano la funzione mapper dell'utente.

Nel reducer, l'organizzazione è per forza di cose più sequenziale: un thread "lettore" preleva le coppie <token, valore> dalla pipe fino all'EOF, poiché la riduzione vera e propria può iniziare solo quando si ha la garanzia di avere l'input completo. Finito l'input e raggruppati i token uguali, il main del processo reducer lancia i propri thread worker per eseguire la riduzione in parallelo sui vari gruppi.

## Struttura delle code interne e meccanismi di sincronizzazione

Per passare i dati dal thread lettore ai worker nel processo mapper ho implementato il classico paradigma produttore-consumatore tramite una coda circolare FIFO limitata dal parametro queue_size.

Per proteggere la memoria della coda da race condition uso un mutex C11 (mtx_t). La sincronizzazione dei thread avviene con due condition variables (cnd_t): not_full e not_empty. Se la coda è piena, il lettore (produttore) aspetta; se la coda è vuota, i worker (consumatori) si sospendono. Quando arriva l'EOF dalla pipe, il lettore setta un flag interno e lancia un cnd_broadcast per sbloccare tutti i worker in attesa, permettendogli di terminare l'esecuzione pulitamente.

## Formato dei messaggi scambiati sulle pipe

Visto che i risultati elaborati e restituiti dalle funzioni utente sono dati opachi che possono contenere byte arbitrari (compreso lo \0), ho evitato l'uso di delimitatori testuali come il \n per separare i dati.

Ho implementato un protocollo binario: ogni record spedito è preceduto da una struttura fissa mr_pair_header_t contenente due campi int (token_len e value_len).

Per assicurarmi che le letture e scritture non vengano tagliate a metà dal kernel o dai segnali (EINTR), ho scritto i wrapper readn() e writen() in utils.c, che ciclano fino al trasferimento dell'esatto numero di byte dichiarato dall'header.

## Struttura dati usata per il raggruppamento per token

Il reader del reducer accumula in ingresso tutte le coppie in un array dinamico allocato in RAM, che cresce tramite realloc.

Arrivato l'EOF, utilizzo la funzione qsort della libreria standard sull'array, confrontando i token con strcmp. Questo ordinamento mette fisicamente vicine in memoria tutte le occorrenze dello stesso token. Dopo il sorting, converto queste chiavi contigue in un array di strutture compatte reduce_group_t (un token associato al vettore dei suoi valori parziali), che passo in pasto ai thread worker per l'invocazione della callback.

## Formato del file di output

L'output è scritto in un file binario. Quando il processo principale riceve i risultati dall'ultima pipe, per garantire che l'output sia sempre deterministico a parità di input, riordina localmente l'array dei record (sempre tramite qsort sui token) prima di salvare.

La scrittura sul disco avviene serializzando sequenzialmente i byte: la dimensione del token (int), i caratteri del token, la dimensione del risultato (int) e infine il risultato opaco calcolato.

## Formato del file di log

Il framework scrive il registro degli eventi nel file (di base mr.log), utilizzando il formato standard: [timestamp] [PID] [evento] messaggio.

Le stringhe vengono composte in modo sicuro usando la funzione snprintf per evitare overflow del buffer. Poiché il padre e i due figli potrebbero voler loggare degli eventi in concorrenza, ho gestito la mutua esclusione sia con i mutex mtx_t (per la concorrenza locale dei thread) sia tramite lock sui record del file usando le syscall fcntl e F_SETLKW, che evitano la sovrapposizione delle righe a livello di file system.

## Descrizione dei test realizzati

Per validare il sistema ho implementato una serie di test nel file test_suite.c che collaudano il framework nei casi limite (edge-case) più che in scenari normali. Ecco i test principali:

Gestione file vuoti e righe malformate: Inserisco un file da 0 byte, righe vuote e file dove l'ultima riga termina direttamente con EOF senza \n. Il test si aspetta che la getline e i parser della pipe non si corrompano.

Test limite lunghezza riga estremo: Forzo il sistema passando stringhe enormi ed esagerate di puro testo senza newline. Questo verifica che l'euristica di sicurezza intervenga scartando i record troppo grossi prima che vengano iniettati nella pipe, proteggendo il sistema da esaurimento memoria (OOM) o deadlock per pipe desincronizzate.

Saturazione forzata della Coda: Setto volontariamente queue_size=1 ed avvio numerosi thread. Questo è uno stress-test che forza il framework a bloccare e sbloccare continuamente i thread con le condition variables, garantendomi l'assenza di race condition e lost wakeup sotto carico estremo.

Error handling percorsi: Verifico che la funzione bloccante mr_start() intercetti errori relativi a permessi mancanti o file non esistenti, ritornando correttamente -1 anziché crashare l'esecuzione.