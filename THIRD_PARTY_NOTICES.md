# Third-Party Notices

## openCypher

GraphArrow includes the openCypher grammar under `vendor/opencypher/` and an
ANTLR-generated parser under `src/grapharrow/interfaces/opencypher/generated/`.

Copyright (c) 2015-2023 "Neo Technology," Network Engine for Objects in Lund AB.
The grammar is distributed under the Apache License, Version 2.0. Its original
copyright, license and attribution notices are retained in the vendored sources.

The openCypher grammar's attribution notice states that the work was created by
the collective efforts of the openCypher community. GraphArrow implements a
subset of the language; it does not claim openCypher conformance or approval by
the openCypher Implementers Group. Cypher is a registered trademark of Neo4j Inc.

## ANA tool protocol

GraphArrow's ANA interface follows the public tool shapes documented by
[Agent-Native Graph](https://paoloalbano.github.io/agent-native-graph/#tool-catalog),
copyright (c) 2026 Paolo Albano. GraphArrow maintains its own runtime and uses
the reference service only for optional HTTP compatibility checks.

Other dependencies retain their respective licenses and notices in their
distributions. This notice does not replace those license texts.
