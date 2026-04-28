#!/usr/bin/env python3
"""
Minimal hosted SochDB + Studio example.

This example does two things:
1. writes a few demo documents to a remote SochDB gRPC collection
2. sends a matching event to the hosted Studio backend

Environment variables:
    SOCHDB_GRPC_ADDRESS   default: studio.agentslab.host:50053
    STUDIO_BASE_URL       default: http://studio.agentslab.host:3000
    STUDIO_API_KEY        required for Studio event ingestion
"""

from __future__ import annotations

import os
import time

from sochdb import SochDBClient, StudioClient


DEFAULT_GRPC_ADDRESS = "studio.agentslab.host:50053"
DEFAULT_STUDIO_BASE_URL = "http://studio.agentslab.host:3000"
DEFAULT_COLLECTION = "sdk_demo_docs"


def main() -> None:
    grpc_address = os.environ.get("SOCHDB_GRPC_ADDRESS", DEFAULT_GRPC_ADDRESS)
    studio_base_url = os.environ.get("STUDIO_BASE_URL", DEFAULT_STUDIO_BASE_URL)
    studio_api_key = os.environ.get("STUDIO_API_KEY")

    run_id = f"sdk-demo-{int(time.time())}"
    client = SochDBClient(grpc_address)

    print(f"Connecting to remote SochDB at {grpc_address}")
    client.create_collection(DEFAULT_COLLECTION, dimension=4, namespace="default", metric="cosine")

    documents = [
        {
            "id": f"{run_id}-doc-1",
            "content": "SochDB Studio can show hosted project activity.",
            "embedding": [1.0, 0.0, 0.0, 0.0],
            "metadata": {"source": "python-sdk", "run_id": run_id, "topic": "studio"},
        },
        {
            "id": f"{run_id}-doc-2",
            "content": "Hosted event ingestion makes Studio feel more like Langfuse.",
            "embedding": [0.0, 1.0, 0.0, 0.0],
            "metadata": {"source": "python-sdk", "run_id": run_id, "topic": "events"},
        },
        {
            "id": f"{run_id}-doc-3",
            "content": "SDK parity work should align remote writes and Studio telemetry.",
            "embedding": [0.0, 0.0, 1.0, 0.0],
            "metadata": {"source": "python-sdk", "run_id": run_id, "topic": "sdk"},
        },
    ]

    inserted_ids = client.add_documents(DEFAULT_COLLECTION, documents)
    print(f"Inserted {len(inserted_ids)} documents into {DEFAULT_COLLECTION}")

    if not studio_api_key:
        print("STUDIO_API_KEY not set; skipping Studio event ingestion.")
        return

    studio = StudioClient(studio_base_url, api_key=studio_api_key)
    result = studio.ingest_events(
        [
            {
                "type": "retrieval",
                "name": "python-sdk-demo",
                "status": "ok",
                "run_id": run_id,
                "metadata": {
                    "collection": DEFAULT_COLLECTION,
                    "inserted_ids": inserted_ids,
                    "grpc_address": grpc_address,
                },
            }
        ],
        source="python-sdk-example",
    )
    print(f"Ingested {result.ingested} Studio event(s)")


if __name__ == "__main__":
    main()
