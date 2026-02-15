#!/bin/bash
#
gcloud functions deploy pdf-summarizer --gen2 --runtime=python311 --region=us-central1 --source=. --entry-point=main_router --trigger-http --allow-unauthenticated --memory=4Gi --timeout=540s --max-instances=200 --cpu=2 --concurrency=2

