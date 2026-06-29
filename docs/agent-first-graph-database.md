# Agent-First Graph Database Direction

Questo documento raccoglie una direzione di prodotto/architettura per GraphArrow:
pensarlo non solo come graph database interrogabile da umani, ma come graph database
ottimizzato per essere usato principalmente da agenti.

L'idea non e' sostituire Cypher. Cypher resta importante per:

- compatibilita' con tool esistenti come driver Neo4j, cypher-shell e Memgraph Lab;
- leggibilita' per utenti umani;
- debug, esplorazione manuale e portabilita' mentale da Neo4j/Memgraph.

Pero' per un agente Cypher non e' sempre la superficie migliore. Un agente non ha
bisogno di una lingua comoda da scrivere a mano; ha bisogno di operazioni stabili,
esplicite, sicure, introspezionabili e facili da comporre.

## Tesi

GraphArrow dovrebbe avere due superfici complementari:

- una superficie human-first, basata su Cypher;
- una superficie agent-first, basata su operazioni intenzionali e machine-readable.

La superficie human-first risponde alla domanda:

```text
Come faccio a esprimere una query in modo familiare e leggibile?
```

La superficie agent-first risponde invece alla domanda:

```text
Qual e' l'azione piu' semplice, sicura e verificabile che un agente deve compiere?
```

## Perche' Cypher non basta per gli agenti

Cypher e' potente, ma per un agente introduce alcune difficolta':

- molte query semanticamente diverse sono testualmente simili;
- e' facile generare una query valida ma costosa;
- write e read possono mescolarsi in modi pericolosi;
- i risultati possono essere troppo grandi o poco strutturati;
- il planner deve dedurre l'intento da una stringa;
- error recovery e retry sono meno deterministici;
- l'agente deve sapere dettagli sintattici invece di scegliere tra azioni.

Un umano apprezza una lingua espressiva. Un agente spesso lavora meglio con una
toolbox piu' stretta, dove ogni tool ha input, output, limiti e failure mode chiari.

## Superficie agent-first

La superficie agent-first dovrebbe essere composta da primitive esplicite.
Esempi:

- `inspect_schema`
- `find_nodes`
- `get_node`
- `expand_neighbors`
- `find_paths`
- `upsert_node`
- `upsert_relationship`
- `attach_facts`
- `delete_subgraph`
- `summarize_subgraph`
- `sample_graph`
- `explain_query_plan`

Queste operazioni possono essere esposte via MCP, REST, CLI o embedded Python.
MCP e' una possibile superficie per agenti, ma non deve diventare un vincolo
architetturale. Se separiamo bene use case e interfacce, le stesse operazioni
possono essere usate anche da una CLI locale, che e' spesso piu' semplice per
test, benchmark, debugging e riproducibilita'.

Internamente possono ancora compilare verso operatori GraphArrow o, quando conviene,
verso il subset Cypher.

Il punto chiave e' che l'agente non dovrebbe sempre dover generare:

```cypher
MATCH (n:Person {id: $id})-[r:KNOWS*1..3]->(m)
WHERE ...
RETURN ...
```

Se l'intento e' espandere vicini, dovrebbe poter chiamare qualcosa come:

```json
{
  "tool": "expand_neighbors",
  "node": {"label": "Person", "key": "id", "value": "p1"},
  "direction": "out",
  "relationship_types": ["KNOWS"],
  "min_depth": 1,
  "max_depth": 3,
  "limit": 100
}
```

Questo e' piu' facile da validare, pianificare, limitare e ottimizzare.

## Principi di design

### 1. Intento prima della sintassi

Un tool agent-first deve rappresentare un'intenzione, non una stringa.

Esempio:

- umano: `MATCH (n:Company {vat: "..."}) RETURN n`
- agente: `get_node(label="Company", key="vat", value="...")`

Il secondo formato dice chiaramente che l'operazione e' un lookup, quindi il motore
puo' scegliere indici, property table, cache o point-lookup path senza interpretare
una query generale.

### 2. Safety by construction

Le operazioni agent-first dovrebbero separare read e write.

Un agente dovrebbe avere tool distinti per:

- lettura;
- scrittura;
- cancellazione;
- import bulk;
- introspezione.

Questo rende piu' semplice applicare policy:

- read-only mode;
- max rows;
- max hops;
- max write count;
- dry-run;
- require confirmation per operazioni distruttive.

### 3. Risultati piccoli, spiegabili e riusabili

Un agente lavora meglio con risultati che includono metadati:

- numero totale stimato;
- numero ritornato;
- flag `truncated`;
- suggerimento per pagina successiva;
- schema delle colonne;
- warnings;
- piano o costo semplificato.

Quindi un output agent-first dovrebbe evitare risposte opache tipo "lista enorme di dict"
quando puo' restituire anche contesto operativo.

### 4. Errori orientati al recupero

Gli errori dovrebbero aiutare l'agente a correggersi.

Invece di:

```text
Unsupported query
```

meglio:

```json
{
  "error": "unsupported_operation",
  "operation": "variable_length_untyped_path",
  "supported_alternatives": [
    "find_paths with max_depth <= 3",
    "expand_neighbors with explicit relationship_types"
  ]
}
```

Questo permette retry automatici piu' affidabili.

### 5. Introspezione come operazione primaria

Un agente deve poter chiedere al database cosa e' possibile fare.

Servono tool dedicati per:

- schema;
- label disponibili;
- relationship type disponibili;
- property frequenti;
- cardinalita';
- esempi di nodi;
- vincoli;
- indici;
- feature Cypher supportate.

Questa e' una differenza importante rispetto a un database pensato solo per umani:
per un agente l'autodescrizione del sistema e' parte del runtime.

## Dove Cypher resta centrale

Cypher resta la superficie principale per:

- compatibilita' con tooling Neo4j-like;
- utenti umani;
- query esplorative libere;
- test leggibili;
- benchmark comparabili;
- fallback quando l'operazione agent-first non copre ancora un caso.

In pratica:

```text
Cypher = lingua generale e compatibile.
Agent tools = API intenzionale e ottimizzabile.
```

Non sono alternative. Sono due porte sullo stesso motore.

## Implicazioni sul planner

Questa direzione aiuta anche il planner.

Con Cypher il planner deve inferire:

- cosa e' un lookup;
- cosa e' un expand;
- cosa e' un path search;
- cosa puo' essere pushdown;
- cosa puo' essere limitato presto.

Con una API agent-first, molte informazioni arrivano gia' strutturate.

Esempio:

- `get_node` puo' andare direttamente verso un point lookup;
- `expand_neighbors` puo' usare adjacency e relationship type pruning;
- `find_paths` puo' scegliere BFS, typed traversal o bounded expansion;
- `upsert_node` puo' usare key constraints e property tables;
- `summarize_subgraph` puo' produrre aggregazioni Arrow-native.

Quindi la superficie agent-first non e' solo ergonomia. E' anche un modo per dare
al motore un piano piu' chiaro prima ancora di avere un optimizer completo.

## Implicazioni sullo storage

Un database usato da agenti dovrebbe ottimizzare alcune operazioni ricorrenti:

- recuperare un nodo per chiave;
- leggere rapidamente le sue proprieta';
- espandere vicini con filtri su tipo relazione;
- trovare piccoli sottografi rilevanti;
- importare fatti incrementalmente;
- deduplicare entita' e relazioni;
- restituire risultati compatti e citabili.

Questo conferma alcune direzioni gia' discusse:

- property table columnar derivate;
- point lookup path piu' avanti;
- adjacency typed per traversal;
- statistiche/catalogo piu' ricchi;
- operatori Arrow-native;
- explain plan leggibile.

## Possibile roadmap

## Set minimo di operazioni agent-first

Il set minimo dovrebbe essere piccolo. L'obiettivo non e' coprire tutto Cypher,
ma dare a un agente una base stabile per capire, leggere, espandere e aggiornare
un knowledge graph senza generare query libere per ogni passo.

### 1. `inspect_schema`

Scopo:

- capire quali label esistono;
- capire quali relationship type esistono;
- capire quali proprieta' sono frequenti;
- leggere cardinalita' e piccoli esempi.

Input indicativo:

```json
{
  "include_samples": true,
  "sample_size": 3
}
```

Output indicativo:

```json
{
  "labels": [
    {"name": "Person", "count": 1200, "sample_properties": ["id", "name"]}
  ],
  "relationship_types": [
    {"name": "WORKS_FOR", "count": 820}
  ],
  "warnings": []
}
```

Perche' serve:

- e' il primo passo quasi sempre;
- evita che l'agente inventi label/proprieta';
- permette all'agente di scegliere i tool successivi in modo informato.

### 2. `find_nodes`

Scopo:

- cercare candidati nodo tramite label, proprieta', testo o filtri semplici;
- restituire risultati piccoli e ordinati.

Input indicativo:

```json
{
  "label": "Company",
  "filters": {"country": "IT"},
  "text": "acme",
  "limit": 20
}
```

Output indicativo:

```json
{
  "nodes": [
    {"id": 42, "labels": ["Company"], "properties": {"name": "Acme Italia"}}
  ],
  "returned_count": 1,
  "truncated": false
}
```

Perche' serve:

- e' il modo base per ancorare una domanda a entita' reali;
- puo' usare property table, full scan controllato o in futuro indici.

### 3. `get_node`

Scopo:

- recuperare un nodo singolo con proprieta' complete;
- opzionalmente includere un piccolo contesto locale.

Input indicativo:

```json
{
  "node": {"label": "Person", "key": "id", "value": "p1"},
  "include_degree": true
}
```

Output indicativo:

```json
{
  "node": {
    "id": 1,
    "labels": ["Person"],
    "properties": {"id": "p1", "name": "Alice"}
  },
  "degree": {"in": 2, "out": 5}
}
```

Perche' serve:

- e' il point lookup agentico;
- rende esplicito che l'agente vuole una singola entita', non una query generale.

### 4. `expand_neighbors`

Scopo:

- espandere vicini da uno o piu' nodi;
- limitare direzione, tipi relazione, profondita' e cardinalita'.

Input indicativo:

```json
{
  "start": {"node_id": 1},
  "direction": "out",
  "relationship_types": ["WORKS_FOR", "OWNS"],
  "min_depth": 1,
  "max_depth": 2,
  "limit": 100
}
```

Output indicativo:

```json
{
  "nodes": [],
  "relationships": [],
  "depth": 2,
  "truncated": false,
  "warnings": []
}
```

Perche' serve:

- e' l'operazione piu' naturale su un KG;
- puo' usare adjacency typed;
- evita query path troppo costose generate male.

### 5. `find_paths`

Scopo:

- trovare percorsi bounded tra due entita';
- limitare depth, relationship type e numero massimo di path.

Input indicativo:

```json
{
  "source": {"node_id": 1},
  "target": {"node_id": 9},
  "relationship_types": ["OWNS", "CONTROLS", "WORKS_FOR"],
  "max_depth": 4,
  "limit": 20
}
```

Perche' serve:

- supporta domande di spiegazione e collegamento;
- e' centrale per agenti investigativi o reasoning-oriented;
- deve essere sempre bounded per sicurezza.

### 6. `get_subgraph`

Scopo:

- estrarre un sottografo piccolo e serializzabile;
- servire UI, report, memoria LLM o reasoning step successivi.

Input indicativo:

```json
{
  "seeds": [{"node_id": 1}, {"node_id": 9}],
  "max_depth": 2,
  "relationship_types": ["OWNS"],
  "node_limit": 200,
  "edge_limit": 500
}
```

Perche' serve:

- un agente non dovrebbe passarsi tabelle enormi;
- il sottografo e' una forma naturale di contesto.

### 7. `upsert_node`

Scopo:

- creare o aggiornare un nodo in modo idempotente;
- usare una chiave stabile.

Input indicativo:

```json
{
  "label": "Person",
  "key": {"name": "external_id", "value": "p1"},
  "properties": {"name": "Alice", "country": "IT"}
}
```

Output indicativo:

```json
{
  "node_id": 1,
  "created": false,
  "updated_properties": ["country"]
}
```

Perche' serve:

- evita duplicati;
- e' piu' sicuro di generare `MERGE` libero;
- prepara bene il path per import e memoria agentica.

### 8. `upsert_relationship`

Scopo:

- creare o aggiornare una relazione idempotente tra due nodi;
- usare endpoint gia' risolti o lookup key-based.

Input indicativo:

```json
{
  "source": {"label": "Person", "key": "id", "value": "p1"},
  "target": {"label": "Company", "key": "id", "value": "c1"},
  "type": "WORKS_FOR",
  "properties": {"role": "CEO"}
}
```

Perche' serve:

- e' il complemento naturale di `upsert_node`;
- copre gran parte degli import KG;
- puo' essere reso atomicamente idempotente.

### 9. `query_readonly`

Scopo:

- lasciare una via d'uscita Cypher read-only;
- utile quando il set agent-first non copre ancora un caso.

Guardrail minimi:

- rifiutare write clauses;
- `max_rows`;
- `max_hops`;
- timeout;
- warning se il piano sembra costoso.

### 10. `dry_run_write`

Scopo:

- simulare un write agentico prima di applicarlo;
- stimare quante entita' verrebbero create, aggiornate o cancellate.

Perche' serve:

- un agente dovrebbe poter verificare prima di modificare;
- e' importante per operazioni bulk e destructive.

## Set minimo consigliato per MVP

Per un primo MVP agent-first sceglierei questi tool:

- `inspect_schema`
- `find_nodes`
- `get_node`
- `expand_neighbors`
- `upsert_node`
- `upsert_relationship`
- `query_readonly`
- `dry_run_write`

Questo set permette gia':

- esplorazione;
- lookup;
- traversal locale;
- import/update idempotente;
- fallback read-only;
- safety prima dei write.

Rimanderei `find_paths` e `get_subgraph` allo slice successivo se vogliamo ridurre
il primo perimetro, ma li terrei gia' nel design perche' influenzano storage e planner.

## Separazione MCP read-only e write

Per MCP la separazione read/write deve essere una regola architetturale, non solo
una convenzione nei nomi. Un agente dovrebbe poter ricevere solo tool di lettura
senza avere alcuna capacita' di modificare il grafo, anche per errore o prompt
injection.

### Profilo MCP read-only

Questo profilo espone solo tool senza side effect. E' il profilo di default per
agenti non fidati, agenti RAG, strumenti di esplorazione e workflow dove GraphArrow
e' usato come knowledge source.

Tool candidati:

- `schema_get`: labels, relationship types, proprieta', vincoli logici.
- `stats_get`: cardinalita', degree summary, distribuzioni leggere.
- `node_search`: ricerca nodi per label, proprieta', testo o score.
- `node_get`: recupero puntuale per id o chiave stabile.
- `node_expand`: espansione relazioni da un nodo con direzione, tipo, depth e limit.
- `path_find`: ricerca path bounded tra nodi o criteri.
- `subgraph_get`: sottografo compatto intorno ad anchor nodes.
- `query_readonly`: Cypher read-only con guard su write clauses, max rows e max hops.
- `explain_query`: parse/plan/costo senza modifiche.
- `validate_write_plan`: valida una proposta di write senza applicarla.

Il profilo read-only deve rifiutare sempre:

- `CREATE`;
- `MERGE`;
- `SET`;
- `DELETE`;
- `DETACH DELETE`;
- `REMOVE`;
- import commit;
- qualunque tool che possa pubblicare una nuova snapshot.

### Profilo MCP read-write

Questo profilo espone anche tool con side effect. Dovrebbe essere usato solo da
agenti trusted, workflow con approval, o processi batch controllati.

Tool candidati:

- `node_create`
- `node_upsert`
- `node_update`
- `node_delete`
- `edge_create`
- `edge_upsert`
- `edge_update`
- `edge_delete`
- `subgraph_import_preview`
- `subgraph_import_commit`
- `batch_write_preview`
- `batch_write_commit`
- `commit_operations`
- `rollback_commit`
- `write_cypher`

Per i tool write, il contratto dovrebbe includere sempre:

- `dry_run` o tool preview separato;
- `idempotency_key`;
- `actor`;
- `reason`;
- limiti su numero di nodi, archi e cancellazioni;
- diff prodotto;
- commit id;
- warning su duplicati o cardinalita' inattese.

### Evitare tool ibridi ambigui

I tool che possono sia leggere sia scrivere sono pericolosi per un agente. Se un
nome puo' implicare entrambe le cose, va spezzato in due tool.

Esempi:

- invece di `resolve_entity`, usare `entity_resolve_candidates` read-only e
  `entity_upsert_resolved` write.
- invece di `import_subgraph`, usare `subgraph_import_preview` read-only e
  `subgraph_import_commit` write.
- invece di `sync_memory`, usare `memory_diff_preview` read-only e
  `memory_commit` write.

Questa separazione rende possibile dare a un agente un capability set minimo:

```toml
[mcp]
profile = "readonly"
enable_cypher = true
enable_writes = false
max_rows = 100
max_hops = 3
```

Oppure un profilo amministrativo:

```toml
[mcp]
profile = "readwrite"
enable_cypher = true
enable_writes = true
require_preview_for_writes = true
require_idempotency_key = true
max_write_nodes = 1000
max_write_edges = 5000
```

### Implicazione per l'implementazione

Nel codice conviene modellare i tool con metadati espliciti:

- `capability`: `read`, `write`, `admin`;
- `side_effects`: `none`, `snapshot_write`, `destructive`;
- `requires_confirmation`: boolean;
- `requires_idempotency_key`: boolean;
- `allowed_in_readonly_profile`: boolean.

Il server MCP dovrebbe filtrare la lista tool in base al profilo, non limitarsi a
controllare al momento della chiamata. In questo modo l'agente non vede proprio i
tool write quando opera in modalita' read-only.

## Operazioni opzionali

Queste operazioni non sono indispensabili nel primo slice, ma diventano importanti
quando GraphArrow viene usato come memoria o reasoning substrate.

### `resolve_entity`

Serve a deduplicare o collegare una mention testuale a un nodo esistente.

Esempio:

```json
{
  "mention": "Apple",
  "candidate_labels": ["Company", "Fruit"],
  "context": "technology company"
}
```

Output utile:

- candidati ordinati;
- score/confidence;
- motivazione breve;
- suggerimento `create_new` se nessun candidato e' buono.

### `attach_observation`

Serve ad aggiungere un fatto osservato con provenienza.

Esempio:

```json
{
  "subject": {"label": "Person", "key": "id", "value": "p1"},
  "predicate": "WORKS_FOR",
  "object": {"label": "Company", "key": "id", "value": "c1"},
  "source": {"document_id": "doc-123", "span": "p1 works for c1"},
  "confidence": 0.82
}
```

Differenza rispetto a `upsert_relationship`:

- `upsert_relationship` aggiorna il grafo operativo;
- `attach_observation` conserva anche evidence, source, confidence e lineage.

### `summarize_subgraph`

Serve a comprimere un sottografo in testo/JSON leggibile da un LLM.

Output possibile:

- nodi principali;
- relazioni principali;
- path rilevanti;
- anomalie;
- domande suggerite.

### `rank_neighbors`

Serve a ordinare vicini non solo per id o proprieta', ma per utilita':

- centralita' locale;
- recency;
- confidence;
- relazione piu' informativa;
- prossimita' a un obiettivo.

### `find_evidence_paths`

Variante domain-oriented di `find_paths`.
Non cerca solo path qualunque, ma path utili a spiegare una risposta.

### `bulk_ingest`

Serve a caricare molte entita'/relazioni in modo dichiarativo.

Input possibile:

```json
{
  "nodes": [
    {
      "label": "Person",
      "key": "id",
      "rows": [{"id": "p1", "name": "Alice"}]
    }
  ],
  "relationships": [
    {
      "type": "WORKS_FOR",
      "source": {"label": "Person", "key": "id", "field": "person_id"},
      "target": {"label": "Company", "key": "id", "field": "company_id"},
      "rows": [{"person_id": "p1", "company_id": "c1", "role": "CEO"}]
    }
  ]
}
```

Questo e' molto piu' agent-friendly di generare decine di query `UNWIND/MERGE`.

### `archive_entity` / `delete_subgraph`

Serve a distinguere cancellazione logica e fisica.

Per agenti consiglierei:

- default: archive/logical delete;
- destructive delete solo con confirmation o policy esplicita;
- sempre `dry_run` disponibile.

## Flusso operativo per un agente

### Flusso 1: Rispondere a una domanda

```text
user question
  -> inspect_schema
  -> find_nodes / resolve_entity
  -> get_node
  -> expand_neighbors oppure find_paths
  -> get_subgraph
  -> summarize_subgraph
  -> risposta con riferimenti a nodi/relazioni
```

Esempio:

```text
"Perche' Alice e' collegata ad Acme?"
```

Passi:

1. `find_nodes` per Alice e Acme.
2. `find_paths` tra Alice e Acme con depth limit.
3. `get_subgraph` sui path trovati.
4. `summarize_subgraph` per produrre spiegazione.
5. Risposta finale con path/evidence.

### Flusso 2: Importare nuovi fatti

```text
input facts
  -> inspect_schema
  -> dry_run_write
  -> upsert_node per entita'
  -> upsert_relationship per relazioni
  -> get_subgraph o inspect_schema per verifica
```

Esempio:

```text
"Aggiungi che Alice lavora per Acme come CEO"
```

Passi:

1. `upsert_node(Person, id=alice)`.
2. `upsert_node(Company, id=acme)`.
3. `upsert_relationship(WORKS_FOR)`.
4. `expand_neighbors(alice)` per verificare.

### Flusso 3: Aggiornare memoria agentica

```text
new observation
  -> resolve_entity
  -> dry_run_write
  -> attach_observation
  -> update confidence/source metadata
```

Qui il grafo non e' solo storage: diventa memoria con provenienza.

### Flusso 4: Esplorazione autonoma

```text
inspect_schema
  -> sample_graph
  -> find_nodes su label interessanti
  -> expand_neighbors con limiti stretti
  -> rank_neighbors
  -> summarize_subgraph
```

Questo flusso serve quando l'agente non ha una domanda precisa ma deve capire il dominio.

### Flusso 5: Operazione rischiosa

```text
proposed destructive/write operation
  -> dry_run_write
  -> explain_operation
  -> require confirmation se supera soglie
  -> execute
  -> verify
```

Esempio guardrail:

- se `delete_subgraph` tocca piu' di 100 nodi, serve conferma;
- se `expand_neighbors` supera `node_limit`, tronca e segnala;
- se `upsert_relationship` non trova endpoint, suggerisce `upsert_node` prima.

## Forma standard delle risposte

Tutti i tool agent-first dovrebbero restituire una forma coerente:

```json
{
  "data": {},
  "summary": {
    "operation": "expand_neighbors",
    "returned_count": 42,
    "total_estimate": 1000,
    "truncated": true
  },
  "warnings": [
    "Result truncated at node_limit=100"
  ],
  "next_actions": [
    {"tool": "find_paths", "reason": "Two candidate entities were found"}
  ]
}
```

Campi consigliati:

- `data`: risultato principale;
- `summary`: metadati operativi;
- `warnings`: limiti, truncation, fallback;
- `next_actions`: suggerimenti machine-readable;
- `debug`: opzionale, solo se richiesto.

## Guardrail minimi

Ogni tool dovrebbe avere limiti espliciti:

- `limit`;
- `node_limit`;
- `edge_limit`;
- `max_depth`;
- `timeout_ms`;
- `read_only`;
- `dry_run`;
- `allow_create`;
- `allow_delete`;
- `require_confirmation`.

I default dovrebbero essere conservativi.

Esempio:

```json
{
  "max_depth": 2,
  "node_limit": 100,
  "edge_limit": 300,
  "timeout_ms": 5000
}
```

## Relazione con Cypher

I tool agent-first non devono duplicare per sempre tutta la logica.
Idealmente:

```text
agent tool
  -> validated operation object
  -> logical operator / planner
  -> Arrow-native executor
```

Cypher dovrebbe seguire un percorso simile:

```text
Cypher
  -> parser
  -> compiler
  -> logical operator / planner
  -> Arrow-native executor
```

La convergenza importante e' sugli operatori interni, non sulla sintassi esterna.

## Confronto con riferimenti esterni

Un giro sui riferimenti esistenti conferma che il set proposto non e' arbitrario.
Le operazioni ricorrono in quattro famiglie: graph store lifecycle, retrieval
agentico, graph analytics e memory/evidence management.

### SPARQL/RDF graph store operations

SPARQL 1.1 Update distingue tra:

- operazioni di update sui dati: insert, delete, delete/insert, load, clear;
- operazioni di graph management: create, drop, copy, move, add.

Questo conferma che nel nostro design servono almeno:

- `upsert_node`;
- `upsert_relationship`;
- `bulk_ingest`;
- `delete_or_archive`;
- `dry_run_write`;
- una separazione forte tra read e write.

Nel mondo RDF queste operazioni sono generali e orientate a triple/graph store.
Nel nostro caso property graph + agent-first, conviene esporle in modo piu'
intenzionale: non "insert arbitrary triples", ma "upsert entity", "upsert relation",
"attach observation", "archive entity".

Riferimento:

- W3C SPARQL 1.1 Update: https://www.w3.org/TR/sparql11-update/

### GraphRAG operations

Microsoft GraphRAG espone tre modalita' importanti:

- Global Search: ragionamento su domande olistiche usando community summaries;
- Local Search: ragionamento su entita' specifiche espandendo vicini e concetti;
- DRIFT Search: local/entity search arricchita con contesto di community.

Questo conferma tre classi di tool:

- `expand_neighbors` e `get_subgraph` per local search;
- `summarize_subgraph` e community summaries per global search;
- `find_paths` / `find_evidence_paths` per reasoning multi-hop.

Per GraphArrow questo suggerisce che l'MVP puo' partire local-first, ma lo storage
dovrebbe gia' lasciare spazio a summary/community layers piu' avanti.

Riferimento:

- Microsoft GraphRAG: https://microsoft.github.io/graphrag/

### Graph analytics operations

Neo4j Graph Data Science organizza gli algoritmi in categorie ricorrenti:

- centrality;
- community detection;
- similarity;
- path finding;
- DAG algorithms;
- node embeddings;
- topological link prediction.

Non tutte sono operazioni agent-first P0. Pero' indicano bene quali tool opzionali
avranno senso quando il prodotto cresce:

- `rank_neighbors` puo' usare centrality o similarity;
- `summarize_subgraph` puo' usare community detection;
- `find_paths` e `find_evidence_paths` mappano su path finding;
- `resolve_entity` puo' usare similarity, embeddings o link prediction;
- `suggest_relationships` potrebbe arrivare da link prediction.

Riferimento:

- Neo4j Graph Data Science algorithms: https://neo4j.com/docs/graph-data-science/current/algorithms/

### MCP memory server operations

Il memory server MCP e' un riferimento pratico per una superficie agentica minimale.
Espone operazioni come:

- create entities;
- create relations;
- add observations;
- delete entities;
- delete observations;
- delete relations;
- read graph;
- search nodes;
- open nodes.

Questo e' molto vicino alla nostra direzione. La differenza e' che GraphArrow dovrebbe
essere piu' database-like e piu' performance-oriented:

- `create_entities` diventa `upsert_node` / `bulk_upsert_nodes`;
- `create_relations` diventa `upsert_relationship` / `bulk_upsert_relationships`;
- `add_observations` diventa `attach_observation`;
- `search_nodes` diventa `find_nodes`;
- `open_nodes` diventa `get_node`;
- `read_graph` diventa `get_subgraph` o `sample_graph`, non full graph di default.

Riferimento:

- MCP memory server: https://github.com/modelcontextprotocol/servers/tree/main/src/memory

### Knowledge extraction and entity linking

La letteratura su knowledge extraction/entity linking divide spesso il problema in:

- estrazione di entita' dal testo;
- generazione candidati da una knowledge base;
- disambiguazione verso l'entita' corretta.

Questo supporta l'idea che `resolve_entity` non sia un dettaglio opzionale marginale:
per agenti che leggono documenti e aggiornano KG, entity resolution/linking diventa
un'operazione centrale.

Inoltre, i lavori su provenance/entity resolution evidenziano l'importanza di
conservare sorgenti, validita', sicurezza e contesto delle affermazioni. Questo
rafforza `attach_observation` come operazione distinta da un semplice upsert.

Riferimenti:

- Entity linking overview: https://en.wikipedia.org/wiki/Entity_linking
- Linking Graph Entities with Multiplicity and Provenance: https://arxiv.org/abs/1908.04464

### Agentic KG construction

I lavori recenti su agenti per KG construction/RAG convergono su un pattern simile:

```text
read/search
  -> verify
  -> construct/update
  -> retrieve with symbolic + vector context
  -> preserve evidence/provenance
```

Questo conferma che il nostro flusso operativo dovrebbe includere:

- `inspect_schema`;
- `find_nodes`;
- `resolve_entity`;
- `dry_run_write`;
- `upsert_node`;
- `upsert_relationship`;
- `attach_observation`;
- `find_paths` / `get_subgraph`;
- `summarize_subgraph`.

Riferimenti:

- RAGA, Reading-And-Graph-building-Agent: https://arxiv.org/abs/2605.17072
- Agentic RAG with Knowledge Graphs: https://arxiv.org/abs/2507.16507

## Sintesi dal confronto esterno

Il confronto suggerisce questa tassonomia stabile:

- `schema/introspection`: capire cosa esiste e cosa e' supportato;
- `lookup/search`: trovare entita' e candidati;
- `entity resolution`: collegare mention a nodi;
- `local traversal`: espandere vicini;
- `path/evidence retrieval`: trovare collegamenti spiegabili;
- `subgraph retrieval`: produrre contesto compatto;
- `mutation/idempotent write`: upsert di nodi e relazioni;
- `observation/provenance`: aggiungere fatti con fonte/confidenza;
- `analytics/ranking`: centrality, similarity, community, ranking;
- `safety`: dry-run, limits, confirmation, truncation.

Rispetto al nostro MVP, cambierei leggermente la formulazione:

- P0 deve includere `inspect_schema`, `find_nodes`, `get_node`, `expand_neighbors`,
  `upsert_node`, `upsert_relationship`.
- P1 deve includere `query_readonly`, `dry_run_write`, `get_subgraph`.
- P2 deve includere `find_paths`, `resolve_entity`, `attach_observation`.
- P3 deve includere `summarize_subgraph`, `rank_neighbors`, community summaries,
  graph analytics e hybrid vector/KG retrieval.

## Agent Operation Commit Model

Il pezzo mancante tra "operazioni agent-first" e "graph database affidabile" e' il
modello di commit delle operazioni.

Un agente non dovrebbe solo modificare il grafo. Ogni modifica dovrebbe diventare
un evento persistente, verificabile, riproducibile e interrogabile.

Il flusso concettuale dovrebbe essere:

```text
agent tool call
  -> validate operation
  -> build operation plan
  -> optional dry-run
  -> execute transaction
  -> append operation commit
  -> publish graph snapshot
  -> update derived views
```

Questa struttura separa tre cose:

- intenzione: cosa voleva fare l'agente;
- effetto: cosa e' cambiato nel grafo;
- materializzazione: quale snapshot/storage rappresenta il nuovo stato.

### Perche' serve

Senza un operation commit model, GraphArrow rischia di salvare solo lo stato finale.
Per un agente non basta.

Un agente, o un umano che controlla un agente, deve poter rispondere a domande come:

- chi ha creato questa relazione?
- quale tool ha prodotto questo nodo?
- da quale documento arriva questa osservazione?
- quale operazione ha modificato questa proprieta'?
- posso ripetere questa operazione senza duplicare dati?
- posso ricostruire lo stato prima di questo import?
- cosa e' cambiato nell'ultimo run?

Queste domande richiedono uno storico delle operazioni, non solo snapshot.

### Operation commit

Un operation commit e' un record append-only che descrive una modifica logica.

Schema indicativo:

```json
{
  "commit_id": "op_2026_06_26_000001",
  "transaction_id": "txn_abc",
  "operation_type": "upsert_relationship",
  "status": "committed",
  "idempotency_key": "WORKS_FOR:Person:p1:Company:c1",
  "actor": {
    "kind": "agent",
    "id": "research-agent-1",
    "tool": "upsert_relationship"
  },
  "input": {
    "source": {"label": "Person", "key": "id", "value": "p1"},
    "target": {"label": "Company", "key": "id", "value": "c1"},
    "type": "WORKS_FOR",
    "properties": {"role": "CEO"}
  },
  "effects": {
    "nodes_created": [],
    "nodes_updated": [],
    "relationships_created": [42],
    "relationships_updated": []
  },
  "provenance": {
    "source_type": "document",
    "source_id": "doc-123",
    "confidence": 0.82
  },
  "snapshot": {
    "previous_snapshot_id": "snap_10",
    "new_snapshot_id": "snap_11"
  },
  "created_at": "2026-06-26T08:00:00Z"
}
```

Campi fondamentali:

- `commit_id`: identificatore stabile del commit;
- `transaction_id`: raggruppa piu' operation commit nello stesso write;
- `operation_type`: tool o classe di operazione;
- `status`: `planned`, `committed`, `failed`, `rolled_back`;
- `idempotency_key`: evita duplicazioni su retry;
- `actor`: agente, umano, sistema o import job;
- `input`: richiesta originale normalizzata;
- `effects`: cosa e' stato creato, aggiornato, cancellato;
- `provenance`: fonte, confidence, document/span, timestamp;
- `snapshot`: collegamento agli snapshot storage.

### Dry-run commit

`dry_run_write` non dovrebbe essere solo una risposta temporanea.
Puo' produrre un commit non applicato:

```json
{
  "commit_id": "dry_001",
  "status": "planned",
  "operation_type": "bulk_ingest",
  "estimated_effects": {
    "nodes_to_create": 120,
    "nodes_to_update": 35,
    "relationships_to_create": 400
  },
  "warnings": [
    "10 rows have missing target endpoint"
  ]
}
```

Questo permette:

- review umana;
- approval workflow;
- debug prima dell'esecuzione;
- confrontare piano stimato ed effetti reali.

### Idempotency key

Ogni write agent-first dovrebbe avere una idempotency key.

Esempi:

- nodo: `node:Person:id:p1`;
- relazione: `rel:Person:p1:WORKS_FOR:Company:c1`;
- osservazione: `obs:doc-123:span-44:WORKS_FOR:p1:c1`;
- bulk import row: hash normalizzato della riga e del mapping.

Senza idempotenza, un agente che ritenta dopo timeout puo' duplicare il grafo.
Con idempotenza, il retry puo' diventare:

- no-op;
- update;
- "already committed";
- conflict se l'input e' diverso per la stessa key.

### Operation log su disco

Oltre agli snapshot Parquet, serve un log append-only.

Possibile layout:

```text
grapharrow_db/
  snapshots/
    snap_000001/
      nodes/
      edges/
      node_properties/
      edge_properties/
      adjacency/
    snap_000002/
      ...
  commits/
    date=2026-06-26/
      commits-000001.jsonl
      commits-000002.jsonl
  transactions/
    txn_abc.json
  derived/
    indexes/
    summaries/
```

Formato iniziale consigliato:

- JSONL per commit log, per debug e append semplice;
- Parquet derivato piu' avanti per query analitiche sullo storico;
- ogni commit include `previous_snapshot_id` e `new_snapshot_id`.

Perche' JSONL all'inizio:

- leggibile;
- facile da appendere;
- facile da ispezionare;
- adatto a debugging dei primi agent tool.

Perche' Parquet dopo:

- query storiche;
- aggregazioni su provenance;
- audit su grandi volumi;
- timeline e diff efficienti.

### Snapshot vs commit

Snapshot e commit rispondono a domande diverse.

Snapshot:

- qual e' lo stato corrente del grafo?
- quali nodi e archi esistono?
- come eseguo query veloci?

Commit:

- perche' lo stato e' cosi'?
- quale operazione ha creato questo fatto?
- posso riprodurre una sequenza di cambiamenti?
- posso fare audit di un agente?

GraphArrow dovrebbe mantenere entrambi.

### Transaction boundary

Una chiamata agente puo' produrre uno o piu' commit logici.

Esempio `bulk_ingest`:

```text
transaction txn_1
  commit upsert_node Person p1
  commit upsert_node Company c1
  commit upsert_relationship WORKS_FOR p1 -> c1
  publish snapshot snap_2
```

Opzioni:

- commit logico per ogni entita'/relazione;
- commit aggregato per batch;
- entrambi: un transaction record con effetti aggregati e dettagli per row.

Consiglio:

- P0/P1: commit aggregato per operazione tool;
- P2/P3: dettaglio row-level opzionale per bulk/import/evidence.

### Provenance ed evidence

`attach_observation` dovrebbe scrivere sia nel grafo sia nel commit log.

Esempio:

```json
{
  "operation_type": "attach_observation",
  "claim": {
    "subject": "Person:p1",
    "predicate": "WORKS_FOR",
    "object": "Company:c1"
  },
  "evidence": {
    "source_id": "doc-123",
    "span_start": 420,
    "span_end": 455,
    "text": "Alice is the CEO of Acme.",
    "confidence": 0.82
  }
}
```

Questo permette a un agente di rispondere non solo "Alice lavora per Acme", ma:

```text
Alice lavora per Acme secondo doc-123, confidence 0.82.
```

### Commit generati da Cypher

Anche Cypher write dovrebbe passare dallo stesso modello.

Esempio:

```json
{
  "operation_type": "cypher_write",
  "input": {
    "query": "MATCH (a)-[:WORKS_FOR]->(c) SET a.active = true",
    "params": {}
  },
  "effects": {
    "nodes_updated": [1, 2, 3]
  }
}
```

Questo evita una separazione pericolosa:

- agent tools tracciati;
- Cypher write non tracciati.

Ogni write deve avere audit, anche se arriva da Bolt, REST, embedded Python o MCP.

### Commit e derived views

Le derived views non sono la fonte di verita'.

Esempi:

- property tables columnar;
- adjacency typed;
- point lookup indexes;
- summaries;
- communities;
- vector indexes.

Il commit log deve indicare se una derived view e':

- aggiornata sincronicamente;
- invalidata;
- aggiornata lazy;
- da ricostruire.

Schema indicativo:

```json
{
  "derived_views": {
    "node_properties/Person": "updated",
    "adjacency/WORKS_FOR": "invalidated",
    "community_summaries": "stale"
  }
}
```

### Recovery e replay

Con operation log + snapshot:

- si puo' ripartire dall'ultimo snapshot valido;
- si possono replayare commit successivi;
- si puo' capire quale commit ha rotto qualcosa;
- si puo' costruire una modalita' audit.

Strategia semplice:

```text
load latest valid snapshot
  -> read commits after snapshot
  -> replay committed operations
  -> ignore failed/planned commits
```

Per GraphArrow, che gia' ragiona per snapshot, il replay non deve essere il path
primario per ogni startup. Ma deve essere possibile per recovery, debug e audit.

### Query sullo storico

In futuro servono operazioni agent-first anche sul commit log:

- `list_commits`;
- `get_commit`;
- `find_commits_by_entity`;
- `explain_entity_history`;
- `diff_snapshots`;
- `rollback_logical`;
- `replay_commits`.

Esempio:

```json
{
  "tool": "explain_entity_history",
  "entity": {"label": "Person", "key": "id", "value": "p1"}
}
```

Risposta:

```json
{
  "entity": "Person:p1",
  "events": [
    {
      "commit_id": "op_1",
      "operation": "upsert_node",
      "changed": ["name"],
      "actor": "agent:research-agent-1"
    },
    {
      "commit_id": "op_2",
      "operation": "attach_observation",
      "changed": ["WORKS_FOR"],
      "source_id": "doc-123"
    }
  ]
}
```

### Commit model MVP

Per non sovraccaricare il primo slice, farei cosi':

P0:

- definire schema `OperationCommit`;
- scrivere JSONL append-only per ogni write agent-first;
- includere `operation_type`, `input`, `effects`, `actor`, `snapshot`;
- idempotency key obbligatoria per upsert.

P1:

- commit anche per Cypher write;
- `dry_run_write` come planned commit;
- `list_commits` e `get_commit`.

P2:

- provenance/evidence strutturata;
- commit row-level per bulk ingest;
- diff snapshot.

P3:

- replay;
- rollback logico;
- query storiche agent-first;
- derived view invalidation policy.

Questa e' probabilmente la parte piu' importante per distinguere GraphArrow come
database per agenti: non solo esegue operazioni, ma conserva il perche', il chi,
il quando e l'effetto di ogni operazione.

## Programma sperimentale: agent tools vs Text-to-Cypher

Questa linea puo' diventare un lavoro di ricerca autonomo. L'ipotesi non e':

```text
Gli agent tools sono piu' comodi di Cypher.
```

L'ipotesi forte e misurabile e':

```text
Dato lo stesso modello, budget e grafo, una superficie agent-first composta da
tool tipizzati puo' ottenere accuratezza, robustezza e latenza migliori rispetto
a un agente che genera direttamente Cypher libero.
```

Il confronto va pensato come un benchmark tra due policy di interazione:

- `Text-to-Cypher policy`: il modello riceve schema + domanda e produce query Cypher.
- `Agent-tools policy`: il modello riceve schema + domanda e sceglie una sequenza di tool
  come `find_nodes`, `expand_neighbors`, `get_node`, `find_paths`, `query_readonly`.

Entrambe le policy devono usare lo stesso modello di frontiera, lo stesso budget di token,
lo stesso database e lo stesso set di domande.

### Research questions

Domande principali:

- Gli agent tools riducono gli errori sintattici rispetto a Text-to-Cypher?
- Gli agent tools riducono gli errori semantici quando lo schema e' ambiguo?
- Gli agent tools migliorano il recupero da errori intermedi?
- Gli agent tools riducono latenza e memoria per classi di query frequenti?
- Gli agent tools migliorano la qualita' quando il modello deve esplorare prima di rispondere?
- Gli agent tools rendono piu' sicure le operazioni write rispetto a Cypher generato?

Domanda piu' ambiziosa:

- Possiamo dimostrare che, su certi benchmark graph QA / KG exploration, la scelta della
  superficie operativa conta quanto o piu' della scelta del modello?

### Baseline da confrontare

Servono almeno quattro baseline.

#### 1. LLM + Neo4j Cypher

Il modello genera Cypher per Neo4j.

Serve per confrontarsi con l'ecosistema esistente:

- Neo4j come motore maturo;
- Cypher completo;
- driver ufficiali;
- eventuali strumenti Text-to-Cypher gia' disponibili.

#### 2. LLM + GraphArrow Cypher

Il modello genera Cypher per GraphArrow.

Serve a separare due effetti:

- limiti del nostro subset Cypher;
- qualita' della strategia Text-to-Cypher indipendentemente dal motore.

#### 3. LLM + GraphArrow agent tools read-only

Il modello usa solo tool read-only.

Serve a misurare:

- accuratezza su domande;
- robustezza;
- riduzione errori sintattici;
- controllo di cardinalita';
- latenza su operazioni note.

#### 4. LLM + GraphArrow agent tools read-write

Il modello usa tool write con preview/commit.

Serve a misurare:

- sicurezza delle modifiche;
- idempotenza;
- duplicati creati;
- auditability;
- capacita' di costruire o aggiornare un KG da input testuale.

### Tipi di task per il benchmark

Il benchmark dovrebbe includere task diversi, non solo domanda-risposta.

#### Task A: Lookup puntuale

Esempi:

- "Trova il personaggio Jon Snow."
- "Qual e' l'episodio con piu' morti?"
- "Trova il prodotto con id X."

Perche' e' importante:

- Cypher richiede sapere label/proprieta' esatte;
- agent tools possono usare `find_nodes` + schema hints;
- GraphArrow puo' ottimizzare point lookup e property table.

#### Task B: Local exploration

Esempi:

- "Mostrami le location collegate a questa morte."
- "Quali entita' sono a un hop da questo nodo?"
- "Espandi solo relazioni `HAPPENED_IN` in uscita."

Confronto:

- Cypher: il modello deve generare pattern corretto.
- Tools: `node_get` + `node_expand`.

#### Task C: Multi-hop reasoning

Esempi:

- "Trova vittime uccise da Jon Snow e dove e' avvenuta la morte."
- "Trova connessioni a 2-3 hop tra due entita'."
- "Trova path che passano per una relazione specifica."

Confronto:

- Cypher libero e' potente ma fragile;
- tool come `find_paths` o `expand_neighbors(depth=3)` limitano lo spazio e rendono il
  piano piu' prevedibile.

#### Task D: Aggregate e ranking

Esempi:

- "Quali episodi hanno piu' morti?"
- "Quali location hanno piu' eventi?"
- "Top relationship types per cardinalita'."

Confronto:

- Text-to-Cypher deve gestire `COUNT`, alias, `ORDER BY`, `LIMIT`;
- agent tools possono avere `stats_get`, `group_count`, o template controllati.

#### Task E: Schema discovery

Esempi:

- "Che tipi di nodo ci sono?"
- "Quali proprieta' ha `Episode`?"
- "Quali relazioni collegano `Death` a `Location`?"

Confronto:

- Cypher spesso usa procedure vendor-specific;
- agent tools possono usare `schema_get` e `stats_get`.

#### Task F: Safe write / KG construction

Esempi:

- "Aggiungi questa osservazione come nodo Evidence collegato alla persona corretta."
- "Importa queste triple evitando duplicati."
- "Aggiorna la confidence di una relazione se gia' esiste."

Confronto:

- Cypher generato puo' duplicare nodi o fare write pericolosi;
- agent tools possono imporre `dry_run`, `idempotency_key`, `upsert`, `commit_operations`.

### Metriche

Le metriche devono coprire qualita', costo e sicurezza.

#### Accuratezza

- `exact_answer_accuracy`: risposta finale uguale al gold.
- `semantic_answer_accuracy`: risposta equivalente anche se forma diversa.
- `result_set_f1`: F1 tra righe/nodi attesi e ritornati.
- `path_f1`: F1 su nodi/relazioni di path attesi.

#### Validita' operativa

- `syntax_error_rate`: query/tool call non valida.
- `runtime_error_rate`: errore durante esecuzione.
- `unsupported_feature_rate`: richiesta fuori dal subset supportato.
- `repair_success_rate`: successo dopo uno o piu' retry.

#### Efficienza

- `latency_ms`: tempo end-to-end.
- `db_execution_ms`: tempo motore, escluso tempo modello.
- `tool_calls_count`: numero di chiamate.
- `tokens_in` e `tokens_out`.
- `rows_scanned` o stima equivalente.
- `peak_memory_mb`.

#### Sicurezza write

- `invalid_write_rate`: write che viola vincoli o schema.
- `duplicate_entity_rate`: entita' duplicate create.
- `destructive_error_rate`: cancellazioni o overwrite non richiesti.
- `dry_run_catches_error_rate`: errori intercettati prima del commit.
- `idempotency_success_rate`: retry senza duplicazioni.

#### Agent usability

- `clarification_needed_rate`: quante volte il modello deve chiedere chiarimenti.
- `schema_misuse_rate`: uso di label/proprieta' inesistenti.
- `overfetch_rate`: risultati troppo grandi o non limitati.
- `recoverability_score`: quanto l'errore aiuta il modello a correggersi.

### Protocollo sperimentale

Ogni esperimento dovrebbe fissare:

- modello;
- temperatura;
- system prompt;
- budget massimo di tool call;
- budget massimo token;
- timeout;
- dataset;
- motore;
- seed per task sampling;
- numero massimo di retry.

Per evitare bias:

- stesso schema testuale dato a entrambe le policy;
- stessi esempi few-shot o zero-shot esplicito;
- nessun accesso agli expected result durante l'esecuzione;
- logging completo di prompt, tool call, query, risultati e errori;
- valutatore separato dal modello che esegue il task.

### Dataset candidati

Primi dataset pratici:

- Game of Thrones deaths, perche' lo stiamo gia' usando con Memgraph Lab;
- ICIJ Pandora Papers, per testare entita', ownership e path investigativi;
- dataset sintetico GraphArrow benchmark, per controllare cardinalita' e scala;
- dataset piccolo custom con gold answers manuali;
- dataset generato da schema + template, per avere molte query controllate.

Per un paper/preprint, servira' poi aggiungere benchmark esterni Text-to-Cypher o
Graph QA gia' noti. L'obiettivo non e' solo battere Neo4j sulle performance, ma
mostrare che la superficie agent-first cambia l'affidabilita' del sistema.

### Benchmark Text2Cypher e riferimenti esterni

La letteratura Text2Cypher e' piu' giovane rispetto a Text2SQL, quindi non esiste
ancora un equivalente universalmente dominante di Spider/BIRD. Pero' stanno emergendo
alcuni riferimenti utili.

#### Text2Cypher combined dataset

Il lavoro "Text2Cypher: Bridging Natural Language and Graph Databases" aggrega,
pulisce e organizza piu' dataset pubblici in circa 44k istanze NL-to-Cypher.

Uso per GraphArrow:

- baseline di training/evaluation per Cypher generation;
- confronto con fine-tuning o prompting Text2Cypher;
- analisi di quali query cadono fuori dal subset Cypher attuale;
- sorgente di template/task da convertire in operazioni agent-first.

Limite:

- non tutte le istanze saranno eseguibili direttamente su GraphArrow;
- bisogna distinguere valutazione sintattica da valutazione execution-based.

Riferimento:

- https://arxiv.org/abs/2412.10064

#### SynthCypher / Auto-Cypher

Auto-Cypher propone una pipeline LLM-supervised per generare dati sintetici di
alta qualita' per Text2Cypher. Il dataset SynthCypher ha circa 29.8k istanze su
domini diversi e query con complessita' variabile.

Uso per GraphArrow:

- costruire una baseline Text2Cypher piu' ampia;
- estrarre task per classi di query: lookup, traversal, aggregate, multi-hop;
- confrontare modelli fine-tuned vs agent-tools policy;
- usare la procedura di verifica come ispirazione per il nostro benchmark generator.

Limite:

- dataset sintetico;
- va controllata la copertura di feature Cypher non ancora supportate da GraphArrow.

Riferimento:

- https://arxiv.org/abs/2412.12612

#### SyntheT2C / MedT2C

SyntheT2C genera coppie query-Cypher tramite prompting e template filling, applicate
a knowledge graph medicali. Produce MedT2C e mostra che il fine-tuning su dati
Text2Cypher sintetici migliora i modelli sul task.

Uso per GraphArrow:

- riferimento per benchmark domain-specific;
- utile per task dove schema e terminologia sono importanti;
- interessante per confrontare agent tools su domini ad alta ambiguita' semantica.

Limite:

- dominio medicale;
- potrebbe essere meno adatto come benchmark general-purpose.

Riferimento:

- https://arxiv.org/abs/2406.10710

#### PIPE-Cypher

PIPE-Cypher e' particolarmente vicino a quello che serve a GraphArrow: non propone
solo un dataset statico, ma una pipeline per generare benchmark Text2Cypher a partire
da un property graph live, seed query o log reali. Include schema profiling, grounding,
validazione eseguibile, governance Cypher, redaction e controlli di diversita'.

Uso per GraphArrow:

- modello per generare benchmark specifici su GoT, Pandora, dataset sintetici e grafi
  reali degli utenti;
- modo per creare task bilanciati per difficolta' e classe di query;
- base metodologica forte per un paper, perche' confronta benchmark statici vs
  benchmark generati dal grafo reale;
- puo' usare agent tool calls come seed, non solo query Cypher.

Limite:

- essendo pipeline, va implementata/adattata;
- bisogna definire bene il judge e la validazione per evitare benchmark troppo facili
  o troppo vicini al generatore.

Riferimento:

- https://arxiv.org/abs/2606.08481

#### Text2Cypher multilingual

Il lavoro su Text2Cypher Across Languages valuta e fine-tuna modelli su domande in
piu' lingue mantenendo le query Cypher originali.

Uso per GraphArrow:

- valutare se gli agent tools riducono il gap tra lingue;
- confrontare italiano/inglese quando il dominio e' lo stesso;
- verificare se tool tipizzati aiutano modelli meno forti in lingue diverse.

Limite:

- non misura direttamente agent-tool policy;
- va adattato al nostro protocollo.

Riferimento:

- https://arxiv.org/abs/2506.21445

#### Spider/BIRD adattati a grafo

Spider e BIRD sono benchmark Text2SQL, non Text2Cypher. Pero' sono importanti come
riferimento metodologico:

- domande natural language;
- schema complessi;
- query composizionali;
- valutazione exact match ed execution accuracy;
- split train/dev/test controllati.

Uso per GraphArrow:

- convertire sottoinsiemi relazionali in property graph;
- confrontare SQL-to-Graph transformations;
- costruire task equivalenti in SQL, Cypher e agent tools;
- misurare quando il grafo rende piu' naturale una domanda e quando invece Cypher
  aggiunge complessita'.

Limite:

- conversione non banale;
- rischio di creare grafi artificiali che non rappresentano veri workload graph.

#### LDBC SNB e FinBench

LDBC Social Network Benchmark e FinBench non sono Text2Cypher benchmark in senso
stretto, ma sono importanti per dati e workload graph.

Uso per GraphArrow:

- generare grafi con scala controllata;
- creare task Text2Cypher e agent-tools sopra workload realistici;
- misurare performance motore separata dalla qualita' del modello;
- usare PIPE-Cypher o pipeline simile per produrre NL-to-query pairs grounded.

Limite:

- non danno automaticamente domande natural language;
- richiedono generazione/validazione dei task.

### Come usarli nel nostro paper

Strategia consigliata:

1. Usare benchmark Text2Cypher esistenti per posizionarci rispetto al task classico.
2. Costruire un benchmark GraphArrow-specific in stile PIPE-Cypher, con task generati
   da grafi reali e validati via execution.
3. Per ogni task, avere due soluzioni gold:
   - una query Cypher;
   - una sequenza canonica di agent tools.
4. Valutare lo stesso modello in due modalita':
   - genera Cypher;
   - sceglie tool agent-first.
5. Separare sempre:
   - qualita' del modello;
   - latenza del database;
   - sicurezza dei write;
   - capacita' di recupero dopo errori.

Il claim piu' interessante non sara' "GraphArrow batte Neo4j sempre". Il claim piu'
forte e difendibile e':

```text
Per certe classi di workload graph-agentic, una superficie di tool tipizzati produce
meno errori, migliore controllabilita' e performance piu' prevedibile rispetto alla
generazione diretta di Cypher, anche con modelli di frontiera.
```

### Classi di confronto con Neo4j

Il confronto con Neo4j va separato in due piani.

#### Piano 1: qualita' agentica

Qui Neo4j e GraphArrow sono backend alternativi.

Domanda:

```text
A parita' di modello, l'agente produce risposte migliori con Cypher libero o con
tool agent-first?
```

Possibili confronti:

- Neo4j + Text-to-Cypher;
- GraphArrow + Text-to-Cypher;
- GraphArrow + agent tools;
- eventualmente Neo4j + wrapper agent tools equivalenti.

#### Piano 2: performance del motore

Qui si misura solo il tempo della query o dell'operazione, non la qualita' del modello.

Domanda:

```text
Per le operazioni agent-first piu' frequenti, GraphArrow puo' battere Neo4j perche'
evita il costo di interpretare query generali e usa operatori Arrow specializzati?
```

Operazioni candidate dove GraphArrow potrebbe essere piu' forte:

- lookup property-columnar;
- batch read di proprieta' tipizzate;
- local expansion bounded;
- aggregate columnar su proprieta';
- schema/statistics query;
- import bulk idempotente da tabelle.

### Claim falsificabili

Un buon paper deve avere claim che possono anche fallire.

Claim candidati:

- Su task schema-heavy, agent tools riducono `schema_misuse_rate` rispetto a Text-to-Cypher.
- Su task local exploration, agent tools riducono `syntax_error_rate` quasi a zero.
- Su task bounded traversal, agent tools riducono latenza p50/p95 rispetto a Cypher generato.
- Su task write, preview+commit riduce duplicati e write errati rispetto a Cypher libero.
- Su modelli piu' piccoli, agent tools riducono il gap rispetto ai modelli di frontiera.

### Artefatti da costruire

Per rendere il lavoro riproducibile servono:

- `benchmarks/agent_tools_vs_cypher/tasks/*.jsonl`
- runner per policy Text-to-Cypher;
- runner per policy agent tools;
- adapter Neo4j;
- adapter GraphArrow;
- evaluator deterministico per result set;
- log completo delle run;
- report Markdown/JSON;
- script per generare tabelle paper-ready.

Formato task suggerito:

```json
{
  "id": "got.location_death_count.top",
  "question": "Which locations have the highest number of deaths?",
  "dataset": "got-deaths",
  "expected": {
    "type": "table",
    "columns": ["location_name", "death_count"],
    "rows": []
  },
  "allowed_profiles": ["readonly"],
  "tags": ["aggregate", "ranking", "schema-grounded"],
  "gold_cypher": "MATCH (l:Location)<-[:HAPPENED_IN]-(d:Death) RETURN l.name, count(d)"
}
```

### Roadmap ricerca

P0:

- definire 30-50 task piccoli su Game of Thrones;
- implementare runner GraphArrow Text-to-Cypher;
- implementare runner GraphArrow agent tools read-only simulato;
- misurare accuracy, error rate, latency.

P1:

- aggiungere Neo4j come baseline;
- aggiungere Pandora Papers;
- introdurre modelli multipli;
- aggiungere retry/repair protocol.

P2:

- task write con preview/commit;
- metriche duplicati/idempotenza;
- benchmark su dataset piu' grandi;
- analisi p50/p95/p99.

P3:

- paper/preprint;
- artifact release;
- dataset/task suite pubblica;
- confronto con framework Text-to-Cypher esistenti.

## Piano implementativo sul codice attuale

Questa sezione traduce la direzione agent-first in un piano operativo per il
codice GraphArrow attuale. L'obiettivo e' procedere senza buttare via l'interfaccia
MCP esistente e senza mescolare troppe responsabilita' nello stesso file.

### Principio architetturale fondamentale

La business logic deve stare nei use case, non nelle interfacce.

Questo significa che un'operazione come `node_search` non deve essere implementata
tre volte in tre adapter diversi. Deve esistere una sola implementazione applicativa,
ad esempio in `AgentGraphUseCase`, e poi piu' superfici possono richiamarla:

```text
CLI command
  -> AgentGraphUseCase.node_search(...)
  -> output adapter JSON

MCP tool call
  -> AgentGraphUseCase.node_search(...)
  -> output adapter MCP

REST endpoint eventuale
  -> AgentGraphUseCase.node_search(...)
  -> output adapter HTTP

Embedded Python
  -> AgentGraphUseCase.node_search(...)
  -> Python object / dict
```

Quindi CLI, MCP, REST ed embedded non sono quattro implementazioni diverse della
stessa feature. Sono quattro adapter sopra la stessa logica di business.

Regola pratica:

- se una funzione decide cosa cercare, come filtrare, come usare property tables o
  adjacency, appartiene al use case;
- se una funzione legge argomenti CLI, valida uno schema MCP, costruisce una response
  HTTP o stampa JSON, appartiene all'interfaccia;
- se una funzione serve sia CLI sia MCP, quasi sicuramente non deve stare in
  `interfaces/`.

Questa separazione rende possibile sviluppare e testare le operazioni agent-first
senza dipendere subito da un server MCP. La CLI diventa un modo comodo per invocare
la stessa funzione che poi verra' esposta come tool MCP.

### Cosa e' verificato oggi

Questa sezione distingue cio' che e' gia' presente nel codice da cio' che e' ancora
roadmap. La distinzione e' importante: il piano deve rimanere operativo e non
confondere feature esistenti con desiderata.

Gia' presente nel codice:

- interfaccia MCP transport-agnostic Cypher-oriented;
- modulo `mcp_cypher.py` per preservare la superficie MCP Cypher-only;
- API embedded `Database` / `Session`;
- REST API sincrona;
- Bolt compatibility per driver Neo4j, cypher-shell e Memgraph Lab;
- bulk ingest da `pyarrow.Table`, pandas, list dict, CSV e Parquet;
- catalog, snapshot manifest, checksum e `current_snapshot.json`;
- derived datasets per label, property projection e `adjacency/by_source`;
- `load_tabular_state()` per read path Arrow-first;
- planner V1 per alcuni shape Cypher;
- benchmark Cypher engine su dataset sintetici;
- test unitari e integration per Cypher, REST, Bolt, storage disk/S3 e bulk ingest.

Ancora non presente:

- server MCP completo con transport standard;
- adapter MCP agent-first `mcp_agent.py`;
- profili MCP o tool profile `readonly` / `readwrite`;
- `AgentGraphUseCase`;
- tool agent-first come `node_search`, `node_get`, `node_expand`;
- CLI agent-first per chiamare gli stessi tool senza MCP;
- `adjacency/by_target`;
- lookup derivati `label + key -> node_id` e `node_id -> row`;
- cursor/pagination robusti per subgraph grandi;
- benchmark agent-tools vs Text-to-Cypher.

Assunzioni da verificare con benchmark:

- i tool agent-first riducono errori rispetto a Cypher generato;
- property projection tables migliorano abbastanza i lookup property-heavy;
- typed adjacency fast path scala meglio dei traversal generici;
- una CLI agent-first basta per i primi benchmark senza introdurre subito server MCP;
- i risultati agent-safe riducono token/output size rispetto a tabelle Cypher raw.

### Stato attuale

Oggi GraphArrow ha gia' una prima interfaccia MCP transport-agnostic in:

```text
src/grapharrow/interfaces/mcp.py
```

Questa interfaccia espone una superficie Cypher-oriented:

- `query`
- `write_cypher`
- `get_schema`
- `get_constraints`
- `get_indexes`

Questa superficie va mantenuta. E' utile per:

- compatibilita' con il lavoro gia' fatto;
- debug e smoke test;
- confronto diretto Text-to-Cypher;
- fallback quando i tool agent-first non coprono ancora un caso;
- utenti o agenti che vogliono esplicitamente parlare Cypher.

Quindi il piano non e' rimuovere MCP attuale, ma separarlo meglio.

### Separazione proposta delle interfacce

La struttura consigliata e':

```text
src/grapharrow/interfaces/
  mcp.py
  mcp_cypher.py
  mcp_agent.py
  agent_cli.py
```

Possibile responsabilita':

- `mcp.py`: modulo di compatibilita' o factory principale, per non rompere import esistenti;
- `mcp_cypher.py`: interfaccia MCP Cypher-only, molto simile a quella attuale;
- `mcp_agent.py`: interfaccia MCP agent-first, basata su tool semantici.
- `agent_cli.py`: CLI locale per invocare le stesse operazioni agent-first senza server MCP.

In questo modo:

- la versione Cypher-only resta disponibile;
- la versione agent-first puo' evolvere senza appesantire `mcp.py`;
- la CLI puo' essere usata subito per test, benchmark e debugging;
- possiamo confrontare facilmente le due superfici nei benchmark;
- il codice resta piu' leggibile.

### Nuovo use case dedicato

La logica agent-first non dovrebbe vivere direttamente dentro `interfaces/mcp_agent.py`.
L'interfaccia MCP dovrebbe solo validare input, scegliere il profilo e serializzare
l'output.

Lo stesso vale per la CLI: `agent_cli.py` non dovrebbe contenere logica di dominio.
Deve solo tradurre argomenti CLI in chiamate al use case e stampare JSON/JSONL
stabile.

La logica di dominio dovrebbe stare in un use case dedicato, ad esempio:

```text
src/grapharrow/use_cases/agent_graph.py
```

Responsabilita' del nuovo use case:

- implementare `schema_get`;
- implementare `node_get`;
- implementare `node_search`;
- implementare `node_expand`;
- piu' avanti implementare `path_find`, `subgraph_get`, `node_upsert`, `edge_upsert`;
- usare Arrow/Parquet e tabelle derivate dove possibile;
- evitare di passare da Cypher quando il tool rappresenta gia' un'intenzione chiara.

Questo evita che MCP diventi un secondo motore applicativo.

### Interfacce senza server MCP

Per le prime iterazioni non serve necessariamente un server MCP. La priorita' e'
definire operazioni pure, tipizzate e testabili.

La stratificazione consigliata e':

```text
AgentGraphUseCase
  -> embedded Python API
  -> CLI agent-first
  -> MCP agent-first
  -> REST agent-first eventuale
```

La CLI e' particolarmente utile per:

- provare i tool senza dover integrare subito un client MCP;
- costruire benchmark riproducibili da shell;
- confrontare output agent-first e query Cypher in script;
- salvare input/output JSONL per analisi e paper;
- debug locale quando un agente produce una sequenza di tool call sbagliata.

Esempi di shape CLI desiderata:

```text
grapharrow agent schema-get --path /tmp/db
grapharrow agent node-search --path /tmp/db --label Character --property name --value "Jon Snow" --max-rows 5
grapharrow agent node-expand --path /tmp/db --node-id 123 --rel-type KILLED --direction out --max-hops 1
grapharrow agent query-readonly --path /tmp/db --query "MATCH (n) RETURN labels(n), count(n)"
```

Il formato output dovrebbe essere lo stesso del futuro MCP agent-first. Questo
evita divergenza tra CLI, test e interfaccia agentica.

### Codice riusabile

Possiamo riusare molto codice gia' presente.

#### Riuso immediato

Da `GraphStateUseCase`:

- `load_tabular_state()`;
- `load_node_property_table(...)`;
- `load_edge_property_table(...)`;
- `load_adjacency_by_source_table(...)`.

Da `GraphRepository`:

- canonical node table;
- canonical edge table;
- node label index table;
- node property tables;
- edge property tables;
- adjacency-by-source tables.

Da MCP attuale:

- definizione tool;
- validazione semplice degli argomenti;
- guardrail `max_rows`;
- guardrail `max_hops`;
- payload con `summary`, `warnings`, `truncated`;
- read-only guard per Cypher fallback.

Da catalog/snapshot:

- schema logico;
- label;
- relationship type;
- property definition;
- constraint;
- manifest dei dataset derivati.

#### Riuso da fare con cautela

`load_state()` e' utile per write e operazioni piccole, ma materializza nodi e archi
in Python. Per i tool read-heavy agent-first va evitato quando possibile.

Per `node_search`, `node_get` e `node_expand` il percorso preferito e':

```text
snapshot/catalog
  -> property tables / label tables / adjacency tables
  -> Arrow filtering/join/take
  -> output agent-safe
```

Non:

```text
load_state()
  -> tuple di NodeRecord/EdgeRecord
  -> loop Python
```

### Profili MCP read-only e read-write

La separazione read/write dovrebbe vivere sia nel modello interno sia nel transport.

Modello interno suggerito:

```python
class McpProfile(StrEnum):
    READONLY = "readonly"
    READWRITE = "readwrite"
```

Ogni tool dovrebbe avere metadati espliciti:

```python
McpToolDefinition(
    name="node_search",
    capability="read",
    side_effects=False,
    profiles=("readonly", "readwrite"),
)
```

Per un tool write:

```python
McpToolDefinition(
    name="node_upsert",
    capability="write",
    side_effects=True,
    profiles=("readwrite",),
)
```

Il filtro del profilo deve avvenire in due punti:

- `list_tools(profile=...)`: l'agente vede solo i tool ammessi;
- `call_tool(..., profile=...)`: il server rifiuta comunque un tool non ammesso.

Questo secondo controllo e' importante: nascondere un tool dalla lista non basta.
Un client puo' comunque provare a chiamarlo direttamente.

### Profilo come query param nel transport MCP

Quando monteremo l'interfaccia MCP su HTTP, il profilo puo' essere selezionato con
un query param:

```text
GET /mcp/tools?profile=readonly
GET /mcp/tools?profile=readwrite
POST /mcp/call?profile=readonly
POST /mcp/call?profile=readwrite
```

Oppure, se il transport MCP usera' un path diverso:

```text
/mcp/readonly
/mcp/readwrite
```

La soluzione con `profile` come query param e' flessibile e semplice da testare.
L'importante e' che il profilo venga trasformato subito in un enum interno e non
rimanga una stringa libera sparsa nel codice.

La CLI puo' usare lo stesso concetto con un flag:

```text
grapharrow agent --profile readonly node-search ...
grapharrow agent --profile readwrite node-upsert ...
```

Anche in CLI il profilo deve essere applicato sia alla lista dei comandi disponibili
sia alla chiamata effettiva. Un comando write non deve poter essere invocato se il
profilo e' `readonly`.

### Tool set iniziale agent-first

Per non aprire troppi fronti, il primo set concreto dovrebbe essere read-only:

- `schema_get`;
- `node_get`;
- `node_search`;
- `node_expand`;
- `query_readonly`.

`query_readonly` puo' essere un fallback Cypher, ma dentro la superficie agent-first
dovrebbe essere chiaramente marcato come fallback generale, non come tool principale.

Il primo set read-write dovrebbe arrivare dopo:

- `node_upsert`;
- `edge_upsert`;
- `properties_set`;
- `bulk_nodes_upsert`;
- `bulk_edges_upsert`;
- `write_preview` o `dry_run_write`.

### Contratto output comune

Tutti i tool agent-first dovrebbero restituire una forma coerente:

```json
{
  "status": "ok",
  "result_type": "nodes",
  "summary": {
    "tool": "node_search",
    "matched_count": 42,
    "returned_count": 20
  },
  "limits": {
    "max_rows": 20,
    "max_hops": null
  },
  "truncated": true,
  "warnings": [
    "Result truncated to max_rows=20."
  ],
  "data": []
}
```

Per risultati grafo:

```json
{
  "status": "ok",
  "result_type": "subgraph",
  "nodes": [],
  "relationships": [],
  "summary": {},
  "limits": {},
  "truncated": false,
  "warnings": []
}
```

Questo formato serve a:

- evitare output enormi e opachi;
- rendere i risultati piu' facili da consumare per un LLM;
- supportare pagination/cursor in futuro;
- confrontare in benchmark tool diversi con metriche comuni.

### Limiti architetturali da tenere presenti

I limiti attuali non bloccano il P0, ma vanno esplicitati.

- Non abbiamo ancora un vero server MCP con transport completo.
- L'interfaccia MCP attuale e' transport-agnostic.
- Non esiste ancora un profilo read-only/read-write.
- Non abbiamo ancora tool agent-first.
- `load_state()` materializza tutto in Python e non va usato per read pesanti.
- Abbiamo adjacency-by-source, ma non ancora adjacency-by-target.
- Incoming expansion puo' richiedere scan degli edge finche' non aggiungiamo un indice derivato.
- Non abbiamo ancora cursor/pagination robusti per subgraph grandi.
- Non abbiamo ancora token-budget estimation per risultati pensati per LLM.
- Non abbiamo ancora un benchmark runner agent-tools vs Text-to-Cypher.

### Sequenza implementativa consigliata

#### Step 1: preservare MCP Cypher-only

- Spostare o duplicare con attenzione l'attuale `mcp.py` in `mcp_cypher.py`.
- Lasciare `mcp.py` come compat wrapper o factory principale.
- Aggiornare i test senza rompere il comportamento esistente.

Risultato atteso:

- `create_mcp_interface(...)` continua a funzionare;
- i 5 tool Cypher-oriented restano disponibili;
- abbiamo un posto pulito dove aggiungere l'interfaccia agent-first.

#### Step 2: introdurre metadata e profili

- Estendere `McpToolDefinition` con metadata:
  - `capability`;
  - `side_effects`;
  - `profiles`;
  - `output_kind`;
  - `default_limits`.
- Introdurre `McpProfile`.
- Implementare filtro su `list_tools(profile=...)`.
- Implementare guard su `call_tool(..., profile=...)`.

Risultato atteso:

- un agente read-only non vede tool write;
- un client non puo' chiamare tool write se il profilo e' read-only.

#### Step 3: creare `AgentGraphUseCase`

- Aggiungere `src/grapharrow/use_cases/agent_graph.py`.
- Collegarlo nel container applicativo.
- Implementare helper comuni per output e limiti.
- Usare `load_tabular_state()` come base, non `load_state()`.

Risultato atteso:

- la logica agent-first e' testabile senza MCP;
- MCP resta un adapter sottile.

#### Step 4: aggiungere CLI agent-first minima

- Aggiungere un comando CLI `agent`.
- Collegare la CLI ad `AgentGraphUseCase`.
- Stampare output JSON stabile identico al futuro MCP agent-first.
- Supportare almeno:
  - `schema-get`;
  - `node-search`;
  - `node-get`;
  - `node-expand`;
  - `query-readonly`.

Risultato atteso:

- possiamo testare e benchmarkare i tool senza server MCP;
- possiamo salvare run in JSONL;
- possiamo confrontare subito CLI agent-first vs Cypher.

#### Step 5: implementare primi tool read-only

Ordine consigliato:

1. `schema_get`
2. `node_search`
3. `node_get`
4. `node_expand`
5. `query_readonly`

Note:

- `node_search` dovrebbe usare property table quando label e property sono presenti;
- `node_get` inizialmente puo' usare `node_id`, poi business key;
- `node_expand` inizialmente puo' supportare bene outgoing typed traversal;
- incoming/bidirectional possono arrivare subito come fallback o dopo `adjacency_by_target`.

#### Step 5: test

Test unitari:

- profilo read-only non lista tool write;
- profilo read-only rifiuta chiamate write;
- profilo read-write lista anche tool mutanti;
- `node_search` rispetta `max_rows`;
- `node_expand` rispetta `max_hops` e `max_edges`;
- output envelope stabile;
- fallback `query_readonly` rifiuta write Cypher.

Test integration:

- piccolo grafo stile Game of Thrones;
- search per label/property;
- expand `Death -> Episode`;
- expand `Character -> Death -> Location` quando supportato;
- confronto con query Cypher equivalente per correttezza.

#### Step 6: MCP agent-first adapter

- Aggiungere `mcp_agent.py`.
- Riutilizzare le stesse definizioni tool e lo stesso output envelope della CLI.
- Aggiungere filtro `profile`.
- Aggiungere test `list_tools` e `call_tool`.

Risultato atteso:

- MCP diventa un adapter sopra use case gia' testati;
- il server MCP resta opzionale e puo' arrivare dopo.

#### Step 7: benchmark minimo

Creare una prima suite locale:

```text
benchmarks/agent_tools_vs_cypher/
  tasks/
  runner_grapharrow_cypher.py
  runner_grapharrow_tools.py
  report.py
```

Prime query/task:

- trovare nodo per label/property;
- espandere vicini typed 1-hop;
- espandere 2-hop bounded;
- conteggio per label;
- recupero sottografo piccolo;
- task Game of Thrones gia' usati in Memgraph Lab.

Metriche:

- correctness;
- latency p50/p95;
- memory;
- numero tool call;
- dimensione output;
- truncation rate;
- error recovery.

#### Step 8: write tools solo dopo

Dopo aver stabilizzato i read tools:

- introdurre `node_upsert`;
- introdurre `edge_upsert`;
- introdurre preview/dry-run;
- introdurre idempotency key;
- introdurre commit/audit log.

Questo evita di esporre troppo presto strumenti mutanti a un agente.

## Priorita' implementativa suggerita

### P0: Base agentica utile

- `inspect_schema`
- `find_nodes`
- `get_node`
- `expand_neighbors`
- `upsert_node`
- `upsert_relationship`

### P1: Safety e fallback

- `query_readonly`
- `dry_run_write`
- forma standard di risposta con `warnings` e `truncated`
- limiti su depth, righe e cardinalita'

### P2: Reasoning graph

- `find_paths`
- `get_subgraph`
- `summarize_subgraph`
- `resolve_entity`

### P3: Memoria ed evidence

- `attach_observation`
- source/confidence metadata;
- evidence path;
- archive/logical delete.

Questa sequenza permette di costruire valore presto senza aspettare un optimizer
completo o una compatibilita' Cypher totale.

### Slice 1: Tool read-only

- `inspect_schema`
- `get_node`
- `find_nodes`
- `expand_neighbors`
- `query_readonly`

Obiettivo: permettere a un agente di esplorare il grafo senza generare Cypher libero
per ogni passo.

### Slice 2: Write controllato

- `upsert_node`
- `upsert_relationship`
- `bulk_upsert_nodes`
- `bulk_upsert_relationships`
- `dry_run_write`

Obiettivo: rendere caricamento e aggiornamento dati piu' sicuri e prevedibili di
query Cypher generate liberamente.

### Slice 3: Explain e guardrail

- `explain_operation`
- `estimate_operation`
- limiti su hop, righe, cardinalita' e write count;
- errori con alternative suggerite.

Obiettivo: fare in modo che l'agente possa pianificare e correggersi.

### Slice 4: Operazioni knowledge-oriented

- `summarize_subgraph`
- `find_evidence_paths`
- `rank_neighbors`
- `resolve_entity`
- `attach_observation`

Obiettivo: spostare GraphArrow da "database interrogabile da agente" a "substrato
di memoria e ragionamento per agenti".

## Posizione consigliata

La direzione consigliata e':

1. continuare a rendere Cypher abbastanza compatibile per tooling umano e benchmark;
2. non inseguire subito tutta la superficie Neo4j;
3. introdurre una API agent-first piccola ma molto ben disegnata;
4. far convergere Cypher e agent tools sugli stessi operatori interni;
5. usare i tool agent-first come modo per accelerare planner, safety e performance.

Questo rende GraphArrow diverso da un clone Neo4j:

```text
Neo4j-like enough for humans and tools.
Arrow-native and agent-first where it matters.
```
