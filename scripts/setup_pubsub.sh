#!/bin/bash
# filepath: /Users/ashyam/Documents/GitHub/bq-sftp-epxort-gcf/setup_pubsub.sh
set -e

# Configuration
PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"insightsprod"}
REGION=${REGION:-"us-central1"}
SERVICE_NAME="boxout-bq-sftp-export"
TOPIC_NAME="boxout-sftp-export-triggers"
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format='value(status.url)')
SERVICE_ACCOUNT="boxout-sftp-export@${PROJECT_ID}.iam.gserviceaccount.com"

# Get project number (needed for Pub/Sub service account)
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format="value(projectNumber)")

echo "🔔 Setting up Pub/Sub for SFTP exports"
echo "Project: ${PROJECT_ID} (${PROJECT_NUMBER})"
echo "Topic: ${TOPIC_NAME}"

# Create the Pub/Sub topic if it doesn't exist
if ! gcloud pubsub topics describe ${TOPIC_NAME} &> /dev/null; then
    echo "📬 Creating Pub/Sub topic ${TOPIC_NAME}..."
    gcloud pubsub topics create ${TOPIC_NAME}
fi

# Create service account if it doesn't exist
if ! gcloud iam service-accounts describe ${SERVICE_ACCOUNT} &> /dev/null; then
    echo "🔑 Creating service account..."
    gcloud iam service-accounts create boxout-sftp-export \
        --description="Service account for SFTP export operations" \
        --display-name="SFTP Export Service Account"
fi

# IMPORTANT: Allow Pub/Sub to create authentication tokens in your project
echo "🔑 Granting token creator role to Pub/Sub service account..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com" \
    --role="roles/iam.serviceAccountTokenCreator" \
    --condition=None

# Grant the service account permission to invoke the Cloud Run service
echo "🔐 Granting permission to invoke Cloud Run service..."
gcloud run services add-iam-policy-binding ${SERVICE_NAME} \
    --region=${REGION} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/run.invoker"

# Delete existing subscription if it exists
SUBSCRIPTION_NAME="${TOPIC_NAME}-subscription"
if gcloud pubsub subscriptions describe ${SUBSCRIPTION_NAME} &> /dev/null; then
    echo "🗑️ Deleting existing subscription ${SUBSCRIPTION_NAME}..."
    gcloud pubsub subscriptions delete ${SUBSCRIPTION_NAME}
fi

# Create a Pub/Sub push subscription to the Cloud Run service with retry controls
echo "🔄 Creating Pub/Sub subscription to Cloud Run..."
gcloud pubsub subscriptions create ${SUBSCRIPTION_NAME} \
    --topic=${TOPIC_NAME} \
    --push-endpoint=${SERVICE_URL} \
    --push-auth-service-account=${SERVICE_ACCOUNT} \
    --ack-deadline=600 \
    --message-retention-duration=1d

echo "✅ Pub/Sub setup completed!"