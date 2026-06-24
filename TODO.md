# GraphArrow TODO

Bozza iniziale della todo list del progetto `grapharrow`.

## Visione

Costruire un property graph database embedded in Python, con storage colonnare basato su PyArrow e Parquet, query language compatibile con Cypher/openCypher, e supporto nativo a filesystem locali e remoti.

## Requisiti già fissati

- Linguaggio principale: Python.
- Storage engine basato su PyArrow.
- Persistenza su Parquet.
- Linguaggio di query: Cypher, prendendo come riferimento openCypher.
- Supporto a storage remoti: S3, Google Cloud Storage, Azure Blob Storage.
- Architettura del codice ispirata a clean architecture.
- Eventual consistency accettabile in alcuni scenari.
- Presenza di un meccanismo ACID.
- Valutare Kuzu come fonte di ispirazione architetturale.
- Valutare GraphAr come formato o layer di persistenza/interoperabilità.

## Direzione architetturale proposta

L'idea iniziale è buona, ma conviene raffinarla in questo modo:

- `Catalog Layer`
  - Gestione schema, label, rel types, property types, constraints e metadata del grafo.
- `Storage Layer`
  - Lettura e scrittura di tabelle Arrow.
  - Materializzazione su Parquet.
  - Integrazione con `pyarrow.fs` per local FS e object storage.
- `Graph Layout Layer`
  - Nodes canonici in dataset unico.
  - Node labels come indici derivati snapshot-specifici.
  - Edge tables canoniche separate per `relationship type`.
  - Adjacency derivate per traversals efficienti.
- `Transaction / Commit Layer`
  - Snapshot immutabili + manifest/versioni + `current_snapshot.json`.
  - Commit `staged + publish`.
  - Commit atomici a livello di dataset, specialmente per object storage.
- `Query Frontend`
  - Parser Cypher.
  - Binder/semantic analyzer.
  - Logical plan.
  - Physical plan.
- `Execution Engine`
  - Scan, filter, expand, join, projection, aggregation.
  - Traversal operators batch-friendly.
- `Index Layer`
  - Primary key index.
  - Label/type index.
  - Adjacency `by_source`.
  - In seguito eventuali secondary/property index.
- `Server Layer`
  - Python embedded API.
  - REST API semplice.
  - Bolt compatibility.
- `Interop Layer`
  - Import/export GraphAr.
  - Import/export CSV/Parquet tabellare.

## Scelte tecniche iniziali consigliate

- Evitare C++ nella prima milestone.
  - Prima MVP in puro Python + PyArrow.
  - Ottimizzazioni native solo dopo profiling reale.
- Non usare da subito Python `dict` come cuore dell'engine.
  - Possono servire per cache o metadata.
  - Il dato canonico dovrebbe restare in Arrow Arrays / Parquet / offset arrays.
- Per gli archi, preferire da subito una rappresentazione tipo CSR/CSC derivata.
  - Kuzu punta molto su storage colonnare + adjacency list sparse.
  - Questo è più coerente con query di espansione che con un semplice edge table piatto.
- Su storage remoti, progettare per snapshot append-only.
  - Su object storage le mutazioni in-place sono scomode.
  - Meglio pensare a commit immutabili con manifest e garbage collection successiva.
- ACID sì, ma con ambizione realistica per una V1.
  - Inizio con transazioni a granularità dataset/snapshot.
  - Obiettivo iniziale: atomic commit, consistent read, isolation semplice.
  - Serializable completo può arrivare dopo.

## TODO per blocchi

### Blocco 0 - Product framing

- [ ] Definire scope MVP.
- [ ] Definire non-goals espliciti della V1.
- [ ] Definire casi d'uso target.
- [ ] Definire dimensioni attese dei grafi.
- [ ] Definire target di performance minimi.
  - [ ] Includere query di traversal rapide fino ad almeno 5 hop.

### Blocco 1 - Fondazioni del repository

- [ ] Inizializzare struttura progetto Python.
- [ ] Scegliere packaging (`pyproject.toml`).
- [ ] Scegliere toolchain base: formatter, linter, type checker, test runner.
- [ ] Definire layout clean architecture del codice.
- [ ] Definire strategia config.
- [ ] Definire logging e tracing.

### Blocco 2 - Modello dati del grafo

- [ ] Definire modello property graph supportato.
- [ ] Definire identificatori di nodo e arco.
  - [x] `logical_id` globali nel singolo database/grafo.
  - [ ] Definire formato concreto dei node IDs.
  - [x] `relationship IDs` espliciti.
  - [ ] Definire formato concreto dei relationship IDs.
- [ ] Definire labels, relationship types e property schema.
  - [x] Supportare nodi `multi-label` stile Neo4j.
  - [x] Tenere le label nel record canonico del nodo.
  - [x] Materializzare `node_labels/<Label>` come indice derivato snapshot-specifico.
  - [x] Codificare `labels` nel record nodo come `list<label_id>`.
  - [ ] Definire semantica dei constraints con nodi multi-label.
  - [x] Tenere `rel_type_id` nel record canonico degli edge.
  - [x] Non tenere `src_label_id` / `dst_label_id` negli edge canonici.
  - [x] Modellare eventuali vincoli sugli endpoint nel catalog/schema.
- [ ] Definire tipi supportati tramite Arrow types.
- [ ] Definire proprietà temporali applicative standard.
  - [ ] `created_at` per archi.
  - [ ] valutare `updated_at`.
  - [ ] valutare `valid_from` / `valid_to`.
- [ ] Documentare la distinzione tra timestamp applicativi e metadati MVCC.
- [ ] Definire schema evolution minima.
- [x] Supportare aggiunta permissiva di nuove property.
- [x] Gestire rimozione property come rimozione logica.
- [x] Vietare type change diretto nella V1.
- [x] Vietare rename diretto nella V1.
- [x] Consentire facilmente nuove label e nuovi relationship type.
- [ ] Definire come rappresentare property deprecated nel catalog.
- [ ] Pianificare alias logici di property come evoluzione futura.
- [ ] Definire constraints iniziali.
  - [x] Primary key / unique key sui nodi supportati nella V1.
    - [x] scope per singola label
    - [x] singola property nella prima versione
  - [x] Required properties supportate nella V1.
    - [x] per label
    - [x] per relationship type
    - [x] semantica semplice: property presente e non-null
  - [x] Endpoint constraints sugli archi supportati nella V1.
    - [x] enforce nel write path
    - [x] con nodi multi-label: almeno una label ammessa su source e target
  - [x] Unicità opzionale supportata nella V1.
    - [x] scope per singola label
    - [x] singola property nella prima versione
  - [x] Nullability supportata esplicitamente nella V1.
    - [x] semantica semplice `nullable=true/false`

### Blocco 3 - Catalog e metadata

- [x] Adottare un catalog versionato separato dai file dati.
- [ ] Definire catalog persistente in dettaglio.
- [ ] Definire registri di `label_id` e `rel_type_id`.
- [ ] Definire `property_schema.json`.
- [ ] Definire `constraints.json`.
- [x] Usare manifest completi, autosufficienti e in JSON.
- [x] Raggruppare i file del manifest per dataset.
- [ ] Definire manifest/versioning del dataset in dettaglio.
- [x] Usare `current_snapshot.json` come puntatore esplicito alla snapshot corrente.
- [x] Includere checksum file-level nel manifest.
- [x] Usare `SHA-256` come algoritmo di checksum di default.
- [ ] Definire quando eseguire la verifica checksum oltre al controllo all'avvio.
- [ ] Definire validazione/schema del manifest JSON.
- [ ] Definire metadata per chunk, file, schema e statistiche.
- [ ] Definire layout directory locale/remoto.
- [ ] Definire metadata minimi per pruning e planning.

### Blocco 4 - Storage engine Arrow/Parquet

- [ ] Definire layout fisico delle node tables.
  - [ ] Valutare se i nodi vivono in una tabella canonica unica con colonne/indici per label membership.
  - [ ] Valutare eventuali strutture derivate per label-specific scans.
- [ ] Definire layout fisico delle edge tables.
- [ ] Definire chunking strategy.
- [ ] Implementare read/write Parquet con PyArrow.
- [ ] Implementare supporto a filesystem locali via `pyarrow.fs`.
- [ ] Implementare supporto S3 via `pyarrow.fs`.
- [ ] Implementare supporto GCS via `pyarrow.fs`.
- [ ] Implementare supporto Azure Blob via `pyarrow.fs`.
- [ ] Gestire compression codec configurabili.
- [ ] Definire convenzioni di naming per file e partizioni.
- [x] Trattare object storage come append-only immutable object store.
- [x] Considerare `current_snapshot.json` come vero commit point anche su object storage.
- [x] Supportare esplicitamente più istanze reader su object storage.
- [x] Usare cleanup esplicito e conservativo su object storage.

### Blocco 5 - Adjacency e indici

- [ ] Definire rappresentazione adjacency primaria.
  - [x] `ordered_by_source` come tabella adjacency semplice nella V1.
  - [ ] Valutare evoluzione futura a rappresentazione CSR-like.
  - [ ] `ordered_by_dest` / CSC-like come evoluzione successiva.
- [ ] Ottimizzare adjacency e layout per query di traversal fino a 5 hop.
- [x] Materializzare `adjacency by_source` eager al commit.
- [ ] Aggiungere `adjacency by_dest` in una milestone successiva.
- [ ] Valutare se `adjacency by_dest` futura debba essere eager, lazy o solo rebuildable.
- [ ] Definire quando materializzare gli indici.
- [ ] Implementare offset arrays per traversals.
- [ ] Implementare primary key lookup per nodi.
- [ ] Implementare label membership index per nodi multi-label.
  - [ ] Dataset `node_labels/<Label>` con sola colonna `node_id`.
  - [ ] Ordinamento per `node_id`.
  - [ ] Chunking per range di `node_id`.
- [ ] Implementare label/type pruning.
- [ ] Valutare zone maps/statistiche per property pruning.
- [ ] Valutare secondary index su proprietà.

### Blocco 6 - Transazioni e consistenza

- [ ] Definire modello transazionale della V1.
- [ ] Definire MVCC basato su `TXID` / `commit_id`, non su timestamp reali.
- [ ] Definire unità di commit.
- [x] Definire commit protocol iniziale basato su `manifest + staged files`.
- [x] Usare staging area per transazione e publication tramite `current_snapshot.json`.
- [ ] Definire cleanup e recovery dei file staged orfani.
- [ ] Valutare introduzione futura di `WAL` o commit log.
- [ ] Definire protocollo di snapshot publication.
- [ ] Assicurare che la snapshot pubblicata includa `adjacency by_source` coerente con gli edge canonici.
- [ ] Gestire rollback in caso di commit fallito.
- [ ] Gestire recovery all'avvio.
- [ ] Definire semantica su object storage.
- [ ] Implementare modello `single-writer` per la V1.
- [ ] Definire come evolvere a `OCC` semplice in una milestone successiva.
- [ ] Definire strategia di garbage collection delle versioni obsolete.
- [ ] Esporre API manuale di `vacuum()` per rimuovere versioni obsolete.
- [ ] Esporre API manuale di `compact()` per compattazione e ricostruzione strutture derivate.

## Design MVCC proposto

### Principi

- Le letture avvengono sempre su una snapshot consistente.
- Le scritture sono append-only.
- Nessuna modifica in-place dei file Parquet esistenti.
- La visibilità delle versioni è governata da `TXID` o `commit_id` monotoni.
- I reader leggono da un manifest fissato all'inizio della transazione.

### Schema logico delle versioni

Ogni nodo e ogni arco hanno:

- un `logical_id` stabile;
- una o più versioni fisiche append-only;
- metadati MVCC per decidere la visibilità.
- per gli archi, un `relationship_id` esplicito e stabile.

Campi consigliati per ogni versione fisica:

- `logical_id`
- `begin_txid`
- `end_txid` nullable
- `is_deleted` opzionale oppure tombstone esplicita
- payload della riga

Per nodi e archi, i timestamp applicativi restano distinti dai metadati MVCC.

Per gli archi è consigliato supportare almeno:

- `created_at`
- opzionalmente `updated_at`
- opzionalmente `valid_from`
- opzionalmente `valid_to`

### Regole di visibilità

Una versione è visibile in una transazione che legge la snapshot `S` se:

- `begin_txid <= S`
- `end_txid is null` oppure `end_txid > S`

### Semantica operazioni

- `INSERT`
  - crea una nuova versione con `begin_txid = commit_txid` e `end_txid = null`
- `UPDATE`
  - chiude la versione precedente con `end_txid = commit_txid`
  - aggiunge una nuova versione con i valori aggiornati
- `DELETE`
  - chiude la versione precedente con `end_txid = commit_txid`
  - opzionalmente scrive una tombstone

### Commit model consigliato

- Il writer scrive nuovi file Parquet staged.
- Il writer produce un nuovo manifest di snapshot.
- Il commit rende visibile la transazione pubblicando il nuovo manifest.
- I reader continuano a usare il manifest precedente fino alla fine della loro transazione.
- La prima iterazione usa `manifest + staged files`, senza `WAL`.
- `WAL + manifest` resta una possibile evoluzione.

### Implicazioni per storage remoto

- Questo modello è compatibile con local FS e object storage.
- L'atomicità pratica del commit vive nella publication del manifest.
- La V1 parte con modello `single-writer`.
- Una milestone successiva potrà introdurre `OCC` semplice.

### Relazione con adjacency e indici

- Node tables e edge tables versionate sono lo storage canonico.
- Adjacency lists, CSR/CSC e secondary index sono strutture derivate.
- Gli indici derivati possono essere ricostruiti o aggiornati a livello di snapshot.

### Compaction

- Nessuna compaction automatica nella V1.
- Esporre API manuali distinte: `vacuum()` e `compact()`.
- `vacuum()` potrà:
  - rimuovere versioni non più visibili.
- `compact()` potrà:
  - riscrivere file piccoli o frammentati;
  - ricostruire adjacency e indici derivati;
  - aggiornare le statistiche del catalog.

### Blocco 7 - Parser Cypher

- [x] Scegliere sorgente grammatica openCypher.
- [x] Usare un parser generator.
- [x] Usare `ANTLR` come scelta implementativa iniziale.
- [x] Tenere `EBNF` come reference grammar e documentazione.
- [x] Definire subset Cypher supportato in MVP.
  - [x] `MATCH`
  - [x] `WHERE`
  - [x] `RETURN`
  - [x] `ORDER BY`
  - [x] `LIMIT`
  - [x] `SKIP`
  - [x] `CREATE`
  - [x] `CREATE` di livello medio:
    - [x] nodo singolo
    - [x] più nodi nella stessa query
    - [x] relazione tra nodi già bindati
    - [x] path lineare semplice
  - [x] `DISTINCT`
  - [x] `COUNT`
  - [x] pattern 1-hop e multi-hop limitati fino a 5 hop
  - [x] variable-length traversal iniziale senza path object completi
- [ ] Implementare parser.
- [ ] Implementare AST.
- [ ] Implementare validazioni sintattiche di base.
- [ ] Pianificare compatibilità progressiva con TCK openCypher.

### Blocco 8 - Semantic analysis e planning

- [ ] Implementare binder.
- [ ] Risolvere variabili, label, rel type e property references.
- [ ] Implementare type checking di base.
- [ ] Definire logical operators.
- [ ] Definire rewrite rules minime.
- [ ] Definire cost model iniziale, anche semplice.
- [ ] Implementare physical planner.
- [ ] Prima evoluzione dopo la MVP: supportare pattern multipli nella stessa `MATCH`.
  - [ ] Partire da pattern multipli collegati da variabili condivise.
  - [ ] Rimandare i cartesian product impliciti più liberi.
- [ ] Evoluzioni future del supporto path:
  - [ ] supportare `p = (...)`
  - [ ] supportare `RETURN p`
  - [ ] supportare `length(p)`
  - [ ] definire meglio deduplicazione/uniqueness dei path

### Blocco 9 - Execution engine

- [ ] Adottare una direzione batch-oriented / Arrow-native per l'engine.
- [ ] Mantenere una V1 ibrida:
  - [ ] scan/filter/projection/aggregation batch-oriented;
  - [ ] expand/traversal batch-friendly con implementazione iniziale più semplice.
- [ ] Implementare table scan per nodi.
- [ ] Implementare edge scan.
- [ ] Implementare filter.
- [ ] Implementare projection.
- [ ] Implementare expand/traversal operator.
- [ ] Implementare join di base.
- [ ] Implementare aggregation.
- [ ] Implementare sort/limit/skip.
- [ ] Implementare pattern matching minimo.
- [ ] Valutare esecuzione batch-oriented su Arrow record batches.

### Blocco 10 - API utente

- [ ] Definire API Python embedded.
- [ ] Definire concetto di `Database` come singolo property graph logico.
- [ ] Definire supporto futuro a più database/grafi nello stesso environment.
- [ ] Definire oggetto `Database`.
- [ ] Definire naming finale tra `Session` e `Connection`.
- [ ] Definire API per query parametrizzate.
- [ ] Definire una API semplice non-Cypher per insert di triple e archi.
- [ ] Definire API di ingest.
- [ ] Definire API di transaction handling.
- [ ] Definire error model coerente.
- [ ] Definire formato dei risultati query e conversione `to_arrow()` / `to_pandas()`.
- [ ] Definire semantica precisa di `add_triple(...)`.
- [x] Prevedere API REST semplice nella V1.
- [x] Prevedere compatibilità Bolt nella V1.
- [x] Definire shape iniziale della REST API.
  - [x] `POST /query`
  - [x] `GET /health`
  - [x] `POST /nodes`
  - [x] `POST /edges`
  - [x] `POST /triples`
- [x] Mantenere la REST API sincrona nella V1.
- [ ] Definire payload e response shape della REST API.
- [ ] Evoluzione futura: progettare una async job API per operazioni lunghe.
- [x] Puntare a compatibilità Bolt solida per il main path dei driver standard.
- [ ] Definire dettagli operativi della compatibilità Bolt.
- [x] Usare autenticazione semplice/opzionale nella V1.
  - [x] embedded senza auth
  - [x] server REST/Bolt con auth minima configurabile
- [ ] Definire dettagli concreti dell'autenticazione lato server.
  - [ ] Definire autenticazione minima lato Bolt.
  - [ ] Definire autenticazione minima lato REST.
- [ ] Valutare compatibilità con Bloom tramite spike dedicato.
- [ ] Mantenere la UX della V1 centrata sullo stato corrente del grafo.
- [ ] Non esporre time-travel nella V1, ma non precluderlo nel design interno.

### Blocco 10b - Server runtime

- [x] Trattare il server come stateless rispetto ai dati persistenti.
- [x] Consentire scaling orizzontale in lettura con più istanze server.
- [x] Mantenere write coordination esplicita nella V1.
- [ ] Definire bootstrap e lifecycle del server REST/Bolt.
- [ ] Definire caching strategy locale per catalog e manifest.
- [ ] Definire coordinazione writer in presenza di più istanze server.
- [ ] Definire limiti, timeout e graceful shutdown del server.

### Blocco 11 - Ingest, import/export e compatibilità

- [ ] Import da pandas/Arrow Table.
- [ ] Import da CSV/Parquet.
- [ ] Export risultati in Arrow / pandas / Parquet.
- [ ] Valutare import bulk del grafo.
- [ ] Valutare loader incrementale.
- [ ] Valutare compatibilità GraphAr.
- [ ] Decidere se GraphAr è:
  - [x] formato di import/export;
  - [ ] eventuale allineamento futuro del layout interno.
- [x] Supportare bulk ingest da `pyarrow.Table`.
- [x] Supportare bulk ingest da `pandas.DataFrame`.
- [x] Supportare bulk ingest da `list[dict]`.
- [x] Normalizzare presto tutto a `pyarrow.Table`.
- [x] Supportare risoluzione esplicita tramite business keys in edge/triple ingest.
- [ ] Definire shape finale delle API bulk.
- [ ] Definire validazione e type conversion Python -> Arrow.

### Blocco 12 - GraphAr

- [ ] Studiare mapping tra schema interno e metadata GraphAr.
- [ ] Studiare mapping tra adjacency interna e adjList GraphAr.
- [ ] Valutare supporto a property groups GraphAr.
- [ ] Valutare supporto a più copie adjacency.
- [ ] Definire limiti del supporto GraphAr in V1.
- [ ] Decidere se supportare scrittura GraphAr da subito o solo lettura.
- [ ] Progettare export GraphAr dal layout interno custom.
- [ ] Progettare import GraphAr verso il layout interno custom.

### Blocco 13 - Testing e qualità

- [ ] Definire test pyramid.
- [ ] Scrivere test unitari per storage.
- [ ] Scrivere test unitari per parser.
- [ ] Scrivere test end-to-end query.
- [ ] Aggiungere dataset di test piccoli ma realistici.
- [ ] Valutare uso parziale del TCK openCypher.
- [ ] Aggiungere property-based tests su storage e traversal.
- [ ] Aggiungere regression tests per compatibilità remota.

### Blocco 14 - Benchmark e profiling

- [ ] Definire benchmark MVP.
- [ ] Confrontare scan Arrow vs alternative.
- [ ] Misurare costi di traversals.
- [ ] Misurare performance di query fino a 5 hop su dataset realistici.
- [ ] Misurare costi di commit su local FS e object storage.
- [ ] Misurare overhead del commit eager per `adjacency by_source`.
- [ ] Confrontare adjacency semplice vs CSR-like.
- [ ] Confrontare layout edge table-only vs CSR-like.
- [ ] Profilare parser/planner/executor.

### Blocco 15 - Documentazione

- [ ] Scrivere README iniziale.
- [ ] Scrivere ADR per decisioni architetturali chiave.
- [ ] Documentare layout storage.
- [ ] Documentare subset Cypher supportato.
- [ ] Documentare limiti transazionali della V1.
- [ ] Documentare compatibilità con GraphAr.

### Blocco 16 - Operations e Admin

- [x] Supportare `health` nella V1.
  - [x] livello medio
  - [x] server vivo
  - [x] config caricata
  - [x] `current_snapshot.json` leggibile
  - [x] manifest attivo leggibile
  - [x] catalog leggibile
  - [x] senza checksum completi nel path di health
- [x] Supportare `verify` nella V1 come operazione admin esplicita.
  - [x] verifica checksum
  - [x] verifica coerenza manifest -> file
  - [x] verifica presenza dei dataset derivati richiesti
  - [x] separata da `health`
- [x] Supportare `recover` nella V1 come operazione admin prudente.
  - [x] invocabile via Python API
  - [x] invocabile via CLI
  - [x] `dry_run`
  - [x] `cleanup_orphan_staged`
  - [x] `verify_checksums`
  - [x] senza rollback amministrativi automatici nella V1
- [x] Esporre `compact()` e `vacuum()` prima via Python API e CLI.
- [ ] Valutare esposizione futura di `compact` / `vacuum` via REST admin.

## MVP consigliata

Per partire bene, la MVP potrebbe includere solo:

- Database embedded locale.
- Node tables e edge tables in Parquet.
- Catalog e manifest versionato.
- Nodes canonici unici con supporto multi-label.
- Edge canonici separati per `relationship type`.
- `node_labels/<Label>` come indice derivato.
- `adjacency by_source` semplice, eager al commit.
- Query subset Cypher:
  - `CREATE`
  - `MATCH`
  - `WHERE`
  - `ORDER BY`
  - `RETURN`
  - `LIMIT`
  - `SKIP`
  - `DISTINCT`
  - `COUNT`
- Traversal base a 1-hop e multi-hop semplice fino a 5 hop.
- Primary key lookup sui nodi.
- Required properties, unique/primary key per-label e endpoint constraints.
- MVCC con `TXID` e snapshot manifest.
- Snapshot commit atomico.
- Read consistency su snapshot.
- Protocollo `staged + publish` con `current_snapshot.json`.
- API manuali di `vacuum()` e `compact()`.
- API Python embedded.
- REST API semplice e sincrona.
- Compatibilità Bolt sul main path dei driver standard.
- Supporto GraphAr solo in import/export, non come storage nativo iniziale.

## Non-goals iniziali consigliati

- Cluster distributed.
- Scritture concorrenti complesse multi-writer su object storage.
- Query optimizer avanzato cost-based.
- Full Cypher compatibility da subito.
- Algoritmi di grafo avanzati nel core engine.
- Estensioni native C++ prima del profiling.

## Decisioni da prendere presto

- [ ] Disegno della futura transizione da `single-writer` a `OCC`.
- [ ] Strategia precisa per frontier expansion e traversals veloci fino a 5 hop.
- [ ] Disegno della futura introduzione di `by_dest`.
- [ ] Criteri di benchmark per decidere se migrare da adjacency semplice a CSR-like.
- [ ] Disegno della prima evoluzione per i pattern multipli nella stessa `MATCH`.
- [ ] Disegno delle future path semantics oltre al traversal multi-hop base.
- [ ] Constraints nella V1 oppure solo metadata?

## Note di valutazione

- L'idea "Arrow Tables + Index Layer + Traversal Engine + Query Interface" è giusta, ma manca il pezzo più delicato: catalog/versioning/transazioni.
- Se vuoi storage remoto serio, il progetto va pensato più come un dataset versionato che come un database che riscrive file in-place.
- GraphAr ha molto senso come formato di interoperabilità e potrebbe anche ispirare il layout fisico, ma probabilmente non conviene bloccare tutta la V1 sul diventare 100% GraphAr-native.
- Kuzu è una buona fonte di ispirazione soprattutto per:
  - storage colonnare;
  - adjacency CSR/CSC-like;
  - query processor vettorizzato;
  - transazioni con WAL e metadata separati.
- Operativamente conviene usare Kuzu come reference architetturale e di benchmark, non come upstream da seguire alla lettera.
