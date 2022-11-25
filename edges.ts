import { createEdgesHasRoute, createEdgesPrecedes, createEdgesServes } from "./main.js"

await createEdgesServes()
await createEdgesPrecedes()
await createEdgesHasRoute()
