# GraphArrow TODO

Bozza iniziale della todo list del progetto `grapharrow`.

## Stato aggiornato

Aggiornato al 2026-06-29 dopo il lavoro su storage, Cypher, Bolt/REST, bulk
ingest, benchmark, documentazione agent-first e primi passaggi di ottimizzazione
Arrow-native sui traversal.

Legenda usata nel documento:

- `[x]`: implementato in modo utilizzabile.
- `[~]`: parziale, implementato solo per alcuni casi o ancora da consolidare.
- `[ ]`: non implementato o ancora solo idea/design.
- `test`: esistono test unitari/integration/benchmark collegati.
- `no test completo`: mancano test dedicati oppure i test coprono solo il caso base.

### Audit sintetico per blocchi

| Blocco | Stato reale | Test | Note |
| --- | --- | --- | --- |
| Product framing | `[~]` | n/a | MVP e direzione sono emersi nei docs, ma target performance e grafi attesi vanno fissati meglio. |
| Fondazioni repo | `[~]` | si | Progetto, packaging, config, test runner e layout ci sono; logging/tracing resta parziale. |
| Modello dati | `[~]` | si | Property graph, multi-label, rel id, constraints e schema evolution base ci sono; timestamp applicativi e property deprecated restano aperti. |
| Catalog/metadata | `[~]` | si | Catalog, manifest, snapshot pointer e checksum ci sono; statistiche/pruning metadata restano aperti. |
| Storage Arrow/Parquet | `[~]` | si | Parquet/local/S3/GCS/Azure path sono coperti; chunking, codec configurabili e dataset stats restano aperti. |
| Adjacency/indici | `[~]` | si | `adjacency/by_source`, `adjacency/by_dest`, label index e property projections esistono; mancano point lookup e offset arrays/CSR-like. |
| Transazioni | `[~]` | si | Snapshot commit, staged publish, explicit Bolt tx e admin recovery ci sono; OCC/WAL/commit log restano futuri. |
| Cypher parser | `[x]` | si | Parser ANTLR openCypher operativo con errori sintattici testati. |
| Semantic/planning | `[~]` | si | C'e' planner V1 per alcuni shape; mancano AST/binder/physical planner generali. |
| Execution engine | `[~]` | si | Esecuzione ibrida Arrow/Python molto estesa; WHERE pushdown traversal, distinct scalar tabellare e frontier variable-length alleggerita presenti, ma executor traversal dedicato resta aperto. |
| API utente | `[~]` | si | Embedded, REST, Bolt, CLI e MCP Cypher-only ci sono; agent-first MCP manca. |
| Server runtime | `[~]` | si | REST/Bolt funzionano; caching, timeout e coordinamento writer multi-instance restano aperti. |
| Ingest/import/export | `[~]` | si | Bulk ingest, CSV/Parquet CLI, pandas/Arrow/list dict ci sono; export e GraphAr restano aperti. |
| GraphAr | `[ ]` | no test completo | Ancora solo direzione/interoperabilita' futura. |
| Testing | `[~]` | si | Molti test unit/integration/moto/Bolt/REST/Cypher; manca test pyramid esplicita e property-based. |
| Benchmark | `[~]` | si | Benchmark Cypher esistono con diagnostica traversal e memoria; mancano benchmark agent-tools vs Cypher e perf CI stabile. |
| Documentazione | `[~]` | n/a | ADR e docs Cypher/planner/agent-first esistono; README/layout/storage docs da rifinire. |
| Operations/Admin | `[x]` | si | Health/verify/recover/compact/vacuum sono presenti anche via REST/CLI/Python. |

### Priorita' attuali

1. Riallineare il motore verso Arrow-native execution dove il payoff e' piu' alto, con focus su traversal executor/adjacency structure.
2. Separare MCP Cypher-only da MCP agent-first senza rompere l'interfaccia attuale.
3. Introdurre profili MCP `readonly` / `readwrite`.
4. Introdurre una CLI agent-first sopra gli stessi use case, senza richiedere subito un server MCP.
5. Aggiungere access path veloci: `label + key -> node_id`, `node_id -> row`, offset arrays/CSR-like.
6. Costruire benchmark agent-tools vs Text-to-Cypher.
7. Aggiornare progressivamente questa checklist quando una voce passa da parziale a completa.

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
  - MCP/agent interfaces come adapter opzionali.
  - CLI agent-first per test e benchmark senza server MCP.
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

- [~] Definire scope MVP. Documentato in forma iniziale in questo file e nei docs Cypher/planner/agent-first.
- [~] Definire non-goals espliciti della V1. Presenti in forma iniziale, da consolidare nel README o in ADR.
- [~] Definire casi d'uso target. Presenti: embedded, REST/Bolt compatibility, bulk ingest, agent-first direction.
- [~] Definire dimensioni attese dei grafi. Usati benchmark 100k/300k nodi, ma manca target ufficiale.
- [~] Definire target di performance minimi. Benchmark presenti, soglie non fissate.
  - [~] Includere query di traversal rapide fino ad almeno 5 hop. Test/benchmark presenti per traversal, ma ottimizzazione ancora aperta.

### Blocco 1 - Fondazioni del repository

- [x] Inizializzare struttura progetto Python. Test: suite unit/integration presente.
- [x] Scegliere packaging (`pyproject.toml`).
- [x] Scegliere toolchain base: formatter, linter, type checker, test runner.
- [~] Definire layout clean architecture del codice. Presente `interfaces`, `use_cases`, `repositories`, `adapters`, ma da rifinire per agent-first.
- [x] Definire strategia config. Presente `GraphArrowSettings`; test REST/Bolt/auth.
- [~] Definire logging e tracing. Logging Bolt presente; tracing/query profiling strutturato ancora aperto.

### Blocco 2 - Modello dati del grafo

- [~] Definire modello property graph supportato. Implementato per labels, rel types, properties, constraints; documentazione da consolidare.
- [~] Definire identificatori di nodo e arco.
  - [x] `logical_id` globali nel singolo database/grafo.
  - [x] Definire formato concreto dei node IDs. Usati `int64` monotoni nel record canonico; test in storage/Cypher/REST.
  - [x] `relationship IDs` espliciti.
  - [x] Definire formato concreto dei relationship IDs. Usati `int64` monotoni nel record canonico; test in storage/Cypher/REST.
- [~] Definire labels, relationship types e property schema.
  - [x] Supportare nodi `multi-label` stile Neo4j.
  - [x] Tenere le label nel record canonico del nodo.
  - [x] Materializzare `node_labels/<Label>` come indice derivato snapshot-specifico.
  - [x] Codificare `labels` nel record nodo come `list<label_id>`.
  - [x] Definire semantica dei constraints con nodi multi-label. Test: `tests/unit/test_constraints.py`.
  - [x] Tenere `rel_type_id` nel record canonico degli edge.
  - [x] Non tenere `src_label_id` / `dst_label_id` negli edge canonici.
  - [x] Modellare eventuali vincoli sugli endpoint nel catalog/schema.
- [~] Definire tipi supportati tramite Arrow types. Property tables derivate e bulk normalization presenti; type system completo ancora aperto.
- [ ] Definire proprietà temporali applicative standard.
  - [ ] `created_at` per archi.
  - [ ] valutare `updated_at`.
  - [ ] valutare `valid_from` / `valid_to`.
- [ ] Documentare la distinzione tra timestamp applicativi e metadati MVCC.
- [~] Definire schema evolution minima.
- [x] Supportare aggiunta permissiva di nuove property.
- [x] Gestire rimozione property come rimozione logica.
- [x] Vietare type change diretto nella V1.
- [x] Vietare rename diretto nella V1.
- [x] Consentire facilmente nuove label e nuovi relationship type.
- [ ] Definire come rappresentare property deprecated nel catalog.
- [ ] Pianificare alias logici di property come evoluzione futura.
- [x] Definire constraints iniziali. Test: `tests/unit/test_constraints.py`.
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
- [x] Definire catalog persistente in dettaglio. Test: catalog/storage/admin.
- [x] Definire registri di `label_id` e `rel_type_id`.
- [x] Definire `property_schema.json`. Implementato come property definitions nel catalog versionato.
- [x] Definire `constraints.json`. Implementato come constraint definitions nel catalog versionato.
- [x] Usare manifest completi, autosufficienti e in JSON.
- [x] Raggruppare i file del manifest per dataset.
- [x] Definire manifest/versioning del dataset in dettaglio. Test: disk/S3/moto/admin.
- [x] Usare `current_snapshot.json` come puntatore esplicito alla snapshot corrente.
- [x] Includere checksum file-level nel manifest.
- [x] Usare `SHA-256` come algoritmo di checksum di default.
- [~] Definire quando eseguire la verifica checksum oltre al controllo all'avvio. `verify` supporta checksum; policy operativa da rifinire.
- [~] Definire validazione/schema del manifest JSON. Parsing typed presente; schema formale da consolidare.
- [~] Definire metadata per chunk, file, schema e statistiche. File metadata/checksum presenti; statistiche/pruning ancora aperti.
- [x] Definire layout directory locale/remoto. Test: disk/S3/GCS/Azure paths.
- [ ] Definire metadata minimi per pruning e planning.

### Blocco 4 - Storage engine Arrow/Parquet

- [x] Definire layout fisico delle node tables. Test: graph repository, disk integration.
  - [x] Valutare se i nodi vivono in una tabella canonica unica con colonne/indici per label membership.
  - [x] Valutare eventuali strutture derivate per label-specific scans. Implementate `node_labels/<Label>` e node property tables.
- [x] Definire layout fisico delle edge tables. Test: graph repository, disk integration.
- [~] Definire chunking strategy. Snapshot/file layout presente; strategia row-group/chunk tuning ancora aperta.
- [x] Implementare read/write Parquet con PyArrow. Test: unit/integration disk/S3.
- [x] Implementare supporto a filesystem locali via `pyarrow.fs`. Test: disk integration.
- [x] Implementare supporto S3 via `pyarrow.fs`. Test: moto + live hook.
- [x] Implementare supporto GCS via `pyarrow.fs`. Test: live hook.
- [x] Implementare supporto Azure Blob via `pyarrow.fs`. Test: live hook.
- [~] Gestire compression codec configurabili. Parquet path presente; policy/config codec da rifinire.
- [x] Definire convenzioni di naming per file e partizioni. Test: snapshot/index metadata.
- [x] Trattare object storage come append-only immutable object store.
- [x] Considerare `current_snapshot.json` come vero commit point anche su object storage.
- [x] Supportare esplicitamente più istanze reader su object storage.
- [x] Usare cleanup esplicito e conservativo su object storage.

### Blocco 5 - Adjacency e indici

- [x] Definire rappresentazione adjacency primaria.
  - [x] `ordered_by_source` come tabella adjacency semplice nella V1.
  - [ ] Valutare evoluzione futura a rappresentazione CSR-like.
  - [x] `ordered_by_dest` eager come tabella adjacency semplice per incoming typed traversal.
  - [ ] Valutare evoluzione futura a rappresentazione CSC-like.
- [~] Ottimizzare adjacency e layout per query di traversal fino a 5 hop. Benchmark e typed adjacency path presenti; structural work ancora aperto.
- [x] Materializzare `adjacency by_source` eager al commit.
- [x] Materializzare `adjacency by_dest` eager al commit.
  - Test: unit storage/manifest, Cypher incoming, Bolt incoming, local/S3 integration.
  - Benchmark: `incoming_typed_projection_order_limit` e diagnostica `incoming_hop_1_anchor_knows`.
- [~] Definire quando materializzare gli indici. `by_source`, label index e property projections eager; policy futura da precisare.
- [ ] Implementare offset arrays per traversals.
- [ ] Implementare primary key lookup per nodi.
- [x] Implementare label membership index per nodi multi-label. Test: snapshot/index metadata, Cypher label scans.
  - [x] Dataset `node_labels/<Label>` con sola colonna `node_id`.
  - [~] Ordinamento per `node_id`. Da verificare come contratto stabile nei test.
  - [ ] Chunking per range di `node_id`.
- [~] Implementare label/type pruning. Presente in parte nel Cypher engine e derived datasets; non ancora optimizer-driven.
- [ ] Valutare zone maps/statistiche per property pruning.
- [~] Valutare secondary index su proprietà. Property projection tables presenti; non ancora point/secondary index generale.

### Blocco 6 - Transazioni e consistenza

- [~] Definire modello transazionale della V1. Snapshot isolation pratica e Bolt explicit tx presenti; modello completo da documentare.
- [~] Definire MVCC basato su `TXID` / `commit_id`, non su timestamp reali. Snapshot pointer/commit txid presenti; versioning row-level non implementato.
- [x] Definire unità di commit. Snapshot manifest publication.
- [x] Definire commit protocol iniziale basato su `manifest + staged files`.
- [x] Usare staging area per transazione e publication tramite `current_snapshot.json`.
- [x] Definire cleanup e recovery dei file staged orfani. Test: CLI/admin recover.
- [ ] Valutare introduzione futura di `WAL` o commit log.
- [x] Definire protocollo di snapshot publication. Test: disk/S3/Bolt tx.
- [x] Assicurare che la snapshot pubblicata includa `adjacency by_source` coerente con gli edge canonici. Test: verify/admin/index metadata.
- [~] Gestire rollback in caso di commit fallito. Bolt transaction rollback presente; rollback amministrativo generale resta prudente/manuale.
- [x] Gestire recovery all'avvio. Initialize/recover/admin path presenti.
- [x] Definire semantica su object storage. Test: moto + live hooks.
- [~] Implementare modello `single-writer` per la V1. Assunzione architetturale presente; lock/coordinamento multi-process da rifinire.
- [ ] Definire come evolvere a `OCC` semplice in una milestone successiva.
- [ ] Definire strategia di garbage collection delle versioni obsolete.
- [x] Esporre API manuale di `vacuum()` per rimuovere versioni obsolete. Test: Python/CLI/REST.
- [x] Esporre API manuale di `compact()` per compattazione e ricostruzione strutture derivate. Test: Python/CLI/REST.

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
- [x] Implementare parser. Test: parser/Cypher syntax errors via `tests/unit/test_cypher.py`.
- [ ] Implementare AST.
- [x] Implementare validazioni sintattiche di base. Test: invalid syntax/unsupported feature.
- [ ] Pianificare compatibilità progressiva con TCK openCypher.

### Blocco 8 - Semantic analysis e planning

- [~] Implementare binder. Semantic checks e scope rules presenti dentro compiler/executor; binder separato mancante.
- [~] Risolvere variabili, label, rel type e property references. Implementato per subset supportato; non ancora pass dedicato.
- [~] Implementare type checking di base. Presente per funzioni/aggregate principali; non generale.
- [~] Definire logical operators. `cypher_plan.py` introduce operatori V1 per alcuni shape.
- [~] Definire rewrite rules minime. Projection/window/distinct/count pushdown parziali nel planner V1.
- [ ] Definire cost model iniziale, anche semplice.
- [~] Implementare physical planner. Planner/executor diretto per shape selezionati; physical planning generale mancante.
- [x] Prima evoluzione dopo la MVP: supportare pattern multipli nella stessa `MATCH`. Test: multi-match e disconnected bounded lookup.
  - [x] Partire da pattern multipli collegati da variabili condivise.
  - [~] Rimandare i cartesian product impliciti più liberi. Supportati solo lookup bounded/disconnected safe.
- [ ] Evoluzioni future del supporto path:
  - [ ] supportare `p = (...)`
  - [ ] supportare `RETURN p`
  - [ ] supportare `length(p)`
  - [ ] definire meglio deduplicazione/uniqueness dei path

### Blocco 8b - Gap Cypher da far crescere in parallelo

Questi punti non bloccano l'agent-first, ma sono importanti per mantenere Cypher
organico, utile per utenti umani, Memgraph Lab/Neo4j-like tooling e benchmark
Text-to-Cypher.

#### P0: Compatibilita' pratica e query generate spesso

- [x] Supportare `RETURN *`.
  - Perche': comune in query generate, debugging e tool grafici.
  - Test richiesti:
    - `MATCH (n) RETURN *`: coperto da espansione scope/entity.
    - `MATCH (a)-[r]->(b) RETURN *`: presente con test Neo4j Recommendations-style.
    - combinazione con `ORDER BY`/`LIMIT` quando possibile: `LIMIT` presente; `ORDER BY` da estendere.
- [ ] Supportare `WITH *`.
  - Perche': comune in pipeline generate e query loader.
  - Test richiesti:
    - `MATCH (n) WITH * RETURN n`;
    - `UNWIND $rows AS row WITH * CREATE ...`;
    - preservazione corretta di entity e scalar bindings.
- [ ] Supportare piu' clausole `WITH` nella stessa query.
  - Perche': molto comune in Cypher generato e script di import.
  - Test richiesti:
    - `MATCH ... WITH ... WITH ... RETURN`;
    - `UNWIND ... WITH ... WITH ... CREATE/MERGE`;
    - errori chiari su casi ancora non supportati.
- [~] Supportare piu' `MATCH` consecutivi prima di `RETURN` e prima di `WITH`.
  - Perche': i tutorial Neo4j Recommendations usano spesso `MATCH ... MATCH ... WITH/RETURN`.
  - Stato: due o piu' `MATCH` prima di `RETURN` supportati; piu' `MATCH` prima di `WITH` ancora aperti.
  - Test richiesti:
    - `MATCH ... MATCH ... RETURN *`: presente;
    - `MATCH ... MATCH ... WITH ... RETURN`: da implementare.
- [~] Ampliare aggregazioni oltre `COUNT`.
  - Perche': Text-to-Cypher e query analitiche usano spesso `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT`.
  - Stato: supportati `SUM`, `AVG`, `MIN`, `MAX` su scalar/property operands e il caso Memgraph
    `round(100 * avg(property))/100`; restano aperti `COLLECT`, `DISTINCT` sugli aggregate non-COUNT,
    cast/funzioni numeriche generiche e aritmetica arbitraria.
  - Test richiesti:
    - aggregazioni scalar-only: presente;
    - aggregazioni grouped: presente;
    - aliases e `ORDER BY` su alias aggregati: presente;
    - fallback/errori per entity aggregation non supportata.
- [~] Supportare `collect(...)` e `collect(DISTINCT ...)`.
  - Perche': centrale nei recommender Neo4j per score components, liste di generi/rating e similarity.
  - Stato: supportato per property/scalar operands in `RETURN` e `WITH`, incluso `DISTINCT`;
    il risultato e' riusabile con `UNWIND`. Restano aperti collect di entity complete,
    ordinamento interno esplicito e casi expression/list piu' complessi.
  - Test richiesti:
    - `WITH rec, collect(g.name) AS genres`: presente;
    - `RETURN collect(DISTINCT g.name) AS genres`: presente;
    - combinazione con `UNWIND`: presente.
- [~] Supportare `NOT EXISTS { pattern }` come anti-semi join.
  - Perche': fondamentale per recommendation queries che escludono item gia' visti/rated.
  - Stato: supportati `EXISTS { pattern }` e `NOT EXISTS { pattern }` per subpattern semplici,
    correlati tramite variabili gia' bound; restano aperte full subquery, piu' clausole interne e
    ottimizzazioni dedicate.
  - Test richiesti:
    - `WHERE NOT EXISTS { (u)-[:RATED]->(rec) }`: presente;
    - versione su pattern con variabili gia' bound;
    - comportamento su optional/missing bindings.
- [~] Supportare relationship type alternatives `[:A|B|C]`.
  - Perche': molto comune nelle query content-based Recommendations su trait multipli.
  - Stato: supportate su fixed-hop match, anche dentro `EXISTS { ... }`/`NOT EXISTS { ... }`;
    restano aperti variable-length + alternatives, dedup semantica piu' esplicita e planner path
    ottimizzati.
  - Test richiesti:
    - fixed-hop outgoing: presente;
    - fixed-hop undirected: da aggiungere;
    - combinazione con variable-length bounded quando disponibile.

#### P1: Pattern e graph result piu' naturali

- [ ] Supportare named path base `p = (...)`.
  - Perche': centrale per graph visualization, path QA e Cypher generato.
  - Test richiesti:
    - `MATCH p=(a)-[r]->(b) RETURN p`;
    - path multi-hop bounded;
    - Bolt graph values se applicabile.
- [ ] Supportare `length(p)`.
  - Perche': spesso accoppiato a named path e shortest/path-style query.
  - Test richiesti:
    - path 1-hop;
    - path multi-hop bounded;
    - `ORDER BY length(p)`.
- [ ] Supportare expression engine numerico/list minimale.
  - Perche': i dataset Recommendations usano `reduce`, list comprehension, `size`, `sqrt`, `abs`,
    potenze, divisioni/moltiplicazioni e accesso a liste/mappe per Jaccard/Cosine/Pearson.
  - Stato: supportati `size(...)` su string/list, arithmetic binaria base `+ - * /`,
    `abs(...)` e confronto `<>`/`!=` nei casi scalar/property testati; restano aperti `reduce`,
    list comprehension, `sqrt`, potenze, map/list indexing e operatori piu' generali.
  - Nota: dopo questo slice fermare l'aggiunta di nuove feature Cypher e passare a stabilizzazione,
    documentazione del perimetro e/o benchmark.
  - Test richiesti:
    - `abs(r1.rating-r2.rating) < 2`: presente;
    - `size(list) > 10`: presente su liste/property e su risultati `collect`;
    - `reduce(...)`;
    - `((1.0*intersection)/size(union)) AS jaccard`.
- [ ] Migliorare relationship variables sui variable-length hops.
  - Perche': molte query vogliono ispezionare relazioni attraversate.
  - Test richiesti:
    - reject chiaro dove resta non supportato;
    - supporto per shape bounded semplici quando implementato.
- [~] Supportare undirected pattern `-[]-` almeno come fallback bidirezionale bounded.
  - Perche': comune in esplorazione grafi e tool visuali.
  - Stato: supportati fixed-hop outgoing/incoming fallback per pattern typed 1-hop; variable-length
    undirected e deduplicazione piu' sofisticata restano aperti.
  - Test richiesti:
    - fixed 1-hop: presente;
    - typed 1-hop: presente;
    - deduplicazione edge/nodi.

#### P2: Compatibilita' tooling e benchmark

- [ ] Supportare `UNION` per read queries semplici.
  - Perche': benchmark e query generate possono usarlo.
  - Test richiesti:
    - union scalar con stesse colonne;
    - errori su colonne incompatibili;
    - `UNION` vs `UNION ALL` se supportato.
- [ ] Ampliare procedure compatibility `CALL`/`SHOW`.
  - Perche': Memgraph Lab, cypher-shell e client Neo4j-like interrogano metadata.
  - Test richiesti:
    - procedure gia' compatibili restano verdi;
    - `SHOW` compatibility dove utile;
    - errori innocui per comandi DDL/tooling ignorabili.
- [ ] Supportare list predicate expressions (`ANY`, `ALL`, `NONE`, `SINGLE`) dove fattibile.
  - Perche': utili in Text-to-Cypher e KG filtering.
  - Test richiesti:
    - predicate su list property;
    - predicate su parametro lista;
    - interazione con `WHERE`.

### Blocco 9 - Execution engine

- [~] Adottare una direzione batch-oriented / Arrow-native per l'engine. In corso; vedere "Blocco 9b".
- [~] Mantenere una V1 ibrida:
  - [~] scan/filter/projection/aggregation batch-oriented;
  - [~] expand/traversal batch-friendly con implementazione iniziale più semplice.
- [x] Implementare table scan per nodi. Test: Cypher/REST/Bolt.
- [x] Implementare edge scan. Test: Cypher/REST/Bolt.
- [x] Implementare filter. Test: WHERE predicates.
- [x] Implementare projection. Test: RETURN/WITH projections.
- [~] Implementare expand/traversal operator. 1-hop/multi-hop typed presenti; WHERE pushdown per hop e variable-length frontier leggera presenti; path object, endpoint-pair executor dedicato e optimizer traversal restano aperti.
- [~] Implementare join di base. Binding joins presenti; operator model generale ancora aperto.
- [~] Implementare aggregation. COUNT/grouped aggregation presenti; Arrow-native grouped path ancora aperto.
- [x] Implementare sort/limit/skip. Test: ORDER BY/LIMIT/SKIP.
- [x] Implementare pattern matching minimo. Test: MATCH/OPTIONAL/MERGE/CREATE variants.
- [~] Valutare esecuzione batch-oriented su Arrow record batches. Tabelle Arrow usate; record batch execution non formalizzata.

### Blocco 9b - Arrow-native execution roadmap

Questa e' la roadmap pratica per ridurre Python nel read path. Il riferimento
architetturale dettagliato e' `docs/cypher-engine-roadmap.md`.

#### Stato attuale

- [x] Introdurre `load_tabular_state()` per evitare materializzazione Python quando possibile. Test: `UNWIND` non carica graph state.
- [x] Usare `orjson` nel tabular adapter per payload JSON interni. Test: unit/benchmark da consolidare.
- [x] Aggiungere property projection tables per nodi e archi. Test: snapshot/index metadata, benchmark property-heavy.
- [x] Usare `adjacency/by_source/<REL_TYPE>` per fast path typed traversal. Test: Cypher traversal e benchmark.
- [~] Plannerizzare alcuni shape `MATCH`, `OPTIONAL MATCH`, `WITH`, `RETURN DISTINCT`, `COUNT`. Test: planner unit + Cypher behavior; `ExpandPlan` supporta pushdown conservativo dei predicati mono-variabile.
- [x] Applicare WHERE pushdown conservativo sui traversal fixed-hop.
  - Stato: filtri mono-variabile in `AND` come `b.active`, `c.country`, `d.score` vengono applicati appena la variabile e' bound.
  - Test: unit planner-on/off e benchmark traversal filtrati.
  - Benchmark: riduzione soprattutto memoria su `three_hop_traversal_dense_where`, `three_hop_traversal_mixed_where`, `multi_match_with_where`.
- [~] Alleggerire variable-length traversal frontier.
  - Stato: la frontier porta un riferimento compatto allo start node e riattacca i campi start solo sulle righe output.
  - Test: range-hop unit e benchmark.
  - Limite: migliora moderatamente memoria/RSS, ma non cambia l'algoritmo di espansione.

#### Hotspot ancora aperti

- [~] Eliminare Python-level `DISTINCT` per risultati scalar/tabular.
  - Stato: `RETURN DISTINCT` non-aggregate su campi scalari semplici con `ORDER BY`/`LIMIT` usa proiezione tabellare + distinct Arrow prima del result shaping.
  - Test: `DISTINCT` prima di `LIMIT` su range-hop.
  - Resta aperto:
    - entity/nested graph results;
    - scalar expressions complesse;
    - `WITH DISTINCT` in tutti gli shape;
    - fallback Python dove Arrow group-by non supporta il tipo.
  - Test richiesti:
    - `RETURN DISTINCT` scalar con `ORDER BY`/`LIMIT` per altri shape;
    - `WITH DISTINCT` scalar;
    - fallback corretto per entity/nested graph results.
- [ ] Spostare grouped `COUNT`/aggregation su Arrow-native group-by quando il risultato e' tabellare.
  - Test richiesti:
    - `RETURN x, COUNT(n)`;
    - `WITH x, COUNT(*) AS total WHERE total > ... RETURN ...`;
    - alias/property expressions come `e.name AS episode_name, COUNT(d)`.
- [ ] Rendere `UNWIND` un operatore columnar invece di loop Python.
  - Test richiesti:
    - `UNWIND [1,2,3]`;
    - `UNWIND $rows AS row WITH row CREATE/MERGE/SET`;
    - `MATCH ... WITH n UNWIND n.list AS value`.
- [ ] Rendere `OPTIONAL MATCH` table-native, non per-row.
  - Test richiesti:
    - optional senza match che conserva row;
    - optional con `WHERE`;
    - `MATCH ... WITH ... OPTIONAL MATCH ... RETURN`;
    - benchmark su fanout crescente.
- [ ] Ridurre materializzazione eager `NodeRecord` / `EdgeRecord` sui read path.
  - Test richiesti:
    - query read-only non chiama `load_state()` quando basta `load_tabular_state()`;
    - write path continua a usare validazione corretta;
    - benchmark memoria prima/dopo.
- [ ] Ridurre `json.loads` / `to_pylist()` nel path caldo.
  - Test richiesti:
    - property projection scalar;
    - label filtering;
    - entity projection fallback;
    - benchmark property-heavy.

#### Access path da aggiungere

- [ ] Derived lookup `label + unique key -> node_id`.
  - Test richiesti:
    - primary key lookup;
    - unique lookup;
    - missing/duplicate key behavior;
    - planner/tool path usa lookup invece di scan.
- [ ] Derived lookup `node_id -> node row`.
  - Test richiesti:
    - direct fetch by id;
    - fetch dopo business-key lookup;
    - benchmark point lookup.
- [x] `adjacency/by_target` concettuale, implementata come `adjacency/by_dest/<REL_TYPE>`, per incoming traversal.
  - Test richiesti:
    - [x] incoming 1-hop;
    - [~] bidirectional fallback resta su path canonico, non usa ancora una struttura dedicata;
    - [x] Memgraph Lab graph result con archi incoming coperto via test Bolt/incoming shape;
    - [x] benchmark incoming expansion.
- [ ] Statistiche minime per pruning/planning.
  - Test richiesti:
    - manifest contiene count/min/max quando disponibile;
    - planner usa stats solo se presenti;
    - verify controlla coerenza metadata.

#### Esperimenti misurati e non mantenuti

- [x] Provare special executor per `RETURN DISTINCT` endpoint pairs su variable-length traversal.
  - Esito: scartato per ora.
  - Motivo: sul benchmark 20k/120k riduceva l'Arrow peak del `range_hop`, ma peggiorava tempo/RSS complessivi; non e' un tradeoff accettabile.
  - Decisione: il prossimo salto richiede una struttura/algoritmo di traversal diverso, non solo un executor speciale sopra join tabellari.

#### Criterio di completamento

Una voce Arrow-native puo' essere marcata `[x]` solo quando:

- il path nuovo e' coperto da unit test;
- esiste almeno un test di fallback/correttezza;
- esiste un benchmark o microbenchmark prima/dopo;
- il risultato non peggiora in modo significativo memoria o latenza sui benchmark esistenti.

### Blocco 10 - API utente

- [x] Definire API Python embedded. Test: `tests/unit/test_database.py`, integration disk/S3.
- [x] Definire concetto di `Database` come singolo property graph logico.
- [ ] Definire supporto futuro a più database/grafi nello stesso environment.
- [x] Definire oggetto `Database`.
- [x] Definire naming finale tra `Session` e `Connection`. Scelto `Session`.
- [x] Definire API per query parametrizzate. Test: Cypher params.
- [x] Definire una API semplice non-Cypher per insert di triple e archi. Test: embedded/REST imperative endpoints.
- [x] Definire API di ingest. Test: bulk ingest.
- [~] Definire API di transaction handling. Embedded read/write sessions e Bolt tx presenti; transazioni generali da rifinire.
- [~] Definire error model coerente. Errori typed presenti; normalizzazione cross-interface da rifinire.
- [ ] Definire formato dei risultati query e conversione `to_arrow()` / `to_pandas()`.
- [~] Definire semantica precisa di `add_triple(...)`. Implementata API triple; doc semantica da rifinire.
- [ ] Definire use case agent-first separato da MCP/REST/CLI.
  - [ ] `AgentGraphUseCase`.
  - [ ] business logic implementata una sola volta nel use case.
  - [ ] CLI, MCP, REST ed embedded come adapter sottili sopra lo stesso use case.
  - [ ] nessun accesso diretto a storage/query planning dentro `interfaces/mcp_agent.py`.
  - [ ] nessun accesso diretto a storage/query planning dentro `interfaces/agent_cli.py`.
  - [ ] output envelope comune.
  - [ ] limiti comuni `max_rows`, `max_hops`, `max_nodes`, `max_edges`.
  - [ ] errori orientati al retry.
- [ ] Definire CLI agent-first per test senza server MCP.
  - [ ] `grapharrow agent schema-get`.
  - [ ] `grapharrow agent node-search`.
  - [ ] `grapharrow agent node-get`.
  - [ ] `grapharrow agent node-expand`.
  - [ ] `grapharrow agent query-readonly`.
  - [ ] flag `--profile readonly|readwrite`.
  - [ ] output JSON/JSONL stabile.
- [ ] Separare MCP Cypher-only da MCP agent-first.
  - [ ] mantenere compatibile `create_mcp_interface(...)`.
  - [x] introdurre `mcp_cypher.py`. Test: `tests/unit/test_mcp.py`.
  - [ ] introdurre `mcp_agent.py`.
  - [ ] introdurre metadata tool `capability`, `side_effects`, `profiles`, `output_kind`, `default_limits`.
  - [ ] filtrare `list_tools(profile=...)`.
  - [ ] validare `call_tool(..., profile=...)`.
- [x] Prevedere API REST semplice nella V1.
- [x] Prevedere compatibilità Bolt nella V1.
- [x] Definire shape iniziale della REST API.
  - [x] `POST /query`
  - [x] `GET /health`
  - [x] `POST /nodes`
  - [x] `POST /edges`
  - [x] `POST /triples`
- [x] Mantenere la REST API sincrona nella V1.
- [x] Definire payload e response shape della REST API. Test: `tests/unit/test_rest.py`, integration disk/S3.
- [ ] Evoluzione futura: progettare una async job API per operazioni lunghe.
- [x] Puntare a compatibilità Bolt solida per il main path dei driver standard.
- [~] Definire dettagli operativi della compatibilità Bolt. Implementazione/test driver presenti; documento operativo da rifinire.
- [x] Usare autenticazione semplice/opzionale nella V1.
  - [x] embedded senza auth
  - [x] server REST/Bolt con auth minima configurabile
- [x] Definire dettagli concreti dell'autenticazione lato server. Test: REST/Bolt auth.
  - [x] Definire autenticazione minima lato Bolt.
  - [x] Definire autenticazione minima lato REST.
- [ ] Valutare compatibilità con Bloom tramite spike dedicato.
- [ ] Mantenere la UX della V1 centrata sullo stato corrente del grafo.
- [ ] Non esporre time-travel nella V1, ma non precluderlo nel design interno.

### Blocco 10b - Server runtime

- [x] Trattare il server come stateless rispetto ai dati persistenti.
- [x] Consentire scaling orizzontale in lettura con più istanze server.
- [x] Mantenere write coordination esplicita nella V1.
- [~] Definire bootstrap e lifecycle del server REST/Bolt. Entrypoint presenti; graceful shutdown/ops docs da rifinire.
- [ ] Definire caching strategy locale per catalog e manifest.
- [ ] Definire coordinazione writer in presenza di più istanze server.
- [ ] Definire limiti, timeout e graceful shutdown del server.

### Blocco 11 - Ingest, import/export e compatibilità

- [x] Import da pandas/Arrow Table. Test: bulk ingest.
- [x] Import da CSV/Parquet. Test: CLI import disk.
- [ ] Export risultati in Arrow / pandas / Parquet.
- [x] Valutare import bulk del grafo. Implementato bulk nodes/edges.
- [~] Valutare loader incrementale. Cypher UNWIND/Memgraph loader shapes parziali; API dedicata ancora aperta.
- [ ] Valutare compatibilità GraphAr.
- [ ] Decidere se GraphAr è:
  - [x] formato di import/export;
  - [ ] eventuale allineamento futuro del layout interno.
- [x] Supportare bulk ingest da `pyarrow.Table`.
- [x] Supportare bulk ingest da `pandas.DataFrame`.
- [x] Supportare bulk ingest da `list[dict]`.
- [x] Normalizzare presto tutto a `pyarrow.Table`.
- [x] Supportare risoluzione esplicita tramite business keys in edge/triple ingest.
- [~] Definire shape finale delle API bulk. API presente; contratto finale/documentazione da consolidare.
- [~] Definire validazione e type conversion Python -> Arrow. Bulk adapter presente; type conversion completa da rifinire.

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

- [~] Definire test pyramid. Suite ampia presente; documento test pyramid mancante.
- [x] Scrivere test unitari per storage. Test: graph state/repository/admin.
- [x] Scrivere test unitari per parser. Test: invalid syntax e parser-backed Cypher.
- [x] Scrivere test end-to-end query. Test: Cypher/REST/Bolt integration.
- [x] Aggiungere dataset di test piccoli ma realistici. Test: medium graph, Game of Thrones style queries.
- [x] Coprire via Bolt le query traversal/range-hop principali. Test: `test_bolt_server_supports_endpoint_range_traversal_queries`.
- [ ] Valutare uso parziale del TCK openCypher.
- [ ] Aggiungere property-based tests su storage e traversal.
- [x] Aggiungere regression tests per compatibilità remota. Test: moto S3 + live object storage hooks.

### Blocco 14 - Benchmark e profiling

- [x] Definire benchmark MVP. Script e report in `scripts/benchmark_cypher_engine.py` e `docs/benchmarks/`.
- [~] Confrontare scan Arrow vs alternative. Benchmark interni presenti; confronto sistematico esterno ancora aperto.
- [x] Misurare costi di traversals. Report benchmark presenti con diagnostica cardinalita' hop-by-hop.
- [x] Misurare performance di query fino a 5 hop su dataset realistici/sintetici. Report benchmark presenti.
- [x] Misurare memoria dei benchmark Cypher. Report include RSS delta e Arrow peak.
- [x] Aggiungere query benchmark per feature Cypher recenti.
  - Include: `collect`, `collect + size`, `NOT EXISTS`, relationship type alternatives, arithmetic/`abs`, multi-match with `WHERE`, wildcard/range-hop.
- [ ] Misurare costi di commit su local FS e object storage.
- [ ] Misurare overhead del commit eager per `adjacency by_source`.
- [ ] Confrontare adjacency semplice vs CSR-like.
- [ ] Confrontare layout edge table-only vs CSR-like.
- [~] Profilare parser/planner/executor. Benchmark query-level presenti; profiler dettagliato per operatori ancora aperto.

### Blocco 15 - Documentazione

- [ ] Scrivere README iniziale.
- [x] Scrivere ADR per decisioni architetturali chiave.
- [~] Documentare layout storage. ADR e docs parziali; manca reference completa aggiornata.
- [x] Documentare subset Cypher supportato. Vedere `docs/cypher-v1.md`.
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
- [x] Valutare esposizione futura di `compact` / `vacuum` via REST admin. Implementato e testato via REST admin.

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
- [~] Strategia precisa per frontier expansion e traversals veloci fino a 5 hop.
  - Fatto: WHERE pushdown, frontier variable-length piu' leggera, diagnostica benchmark.
  - Aperto: executor/struttura adjacency dedicata per endpoint reachability e path expansion.
- [x] Prima introduzione di `by_dest` semplice/eager.
  - Aperto: decidere se e quando passare a CSC-like con offset arrays e pruning sul target.
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
