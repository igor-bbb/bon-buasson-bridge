# VECTRA Deploy Package

Release: FOUNDATION-0012 Professional Knowledge Readback Runtime

This package extends VECTRA Laboratory Knowledge / Self Evolution Actions with official Professional Knowledge readback commands.

OpenAPI Actions after deploy:
- Core: `/vectra/laboratory/openapi/core.json`
- Business Data: `/vectra/laboratory/openapi/business-data.json`
- Knowledge / Self Evolution: `/vectra/laboratory/openapi/knowledge.json`

FOUNDATION-0012 endpoints:
- `GET /vectra/knowledge/professional`
- `GET /vectra/knowledge/professional/overview`
- `GET /vectra/knowledge/professional/{knowledge_id}`
- `GET /vectra/knowledge/professional/{knowledge_id}/readback`

All Professional Knowledge readback operations are read-only and do not modify Professional Model.
