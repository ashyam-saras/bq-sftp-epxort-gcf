#!/bin/bash
set -e

# Configuration
PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"insightsprod"}
REGION=${REGION:-"us-central1"}
TOPIC_NAME="boxout-sftp-export-triggers"
SERVICE_ACCOUNT="boxout-sftp-export@${PROJECT_ID}.iam.gserviceaccount.com"

echo "🔔 Setting up Cloud Schedulers for SFTP exports"
echo "Project: ${PROJECT_ID}"
echo "Pub/Sub Topic: ${TOPIC_NAME}"
echo "Schedule: 1:00 PM IST daily"

# Check if service account exists, create if it doesn't
if ! gcloud iam service-accounts describe ${SERVICE_ACCOUNT} &> /dev/null; then
    echo "🔑 Creating service account..."
    gcloud iam service-accounts create boxout-sftp-export \
        --description="Service account for SFTP export operations" \
        --display-name="SFTP Export Service Account"
fi

# Grant the service account permission to publish to Pub/Sub
echo "🔐 Granting permission to publish to Pub/Sub..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/pubsub.publisher" \
    --condition=None

# Read export configs from JSON file
EXPORTS=$(cat configs/default.json | jq -r '.exports | keys[]')

# Create a scheduler for each export
for EXPORT_NAME in $EXPORTS; do
    SCHEDULER_ID="boxout-sftp-export-${EXPORT_NAME}"
    SCHEDULER_DESC="Daily SFTP export for ${EXPORT_NAME} at 1:00 PM IST"
    SCHEDULE="0 13 * * *"  # 1:00 PM in IST timezone
    
    echo "📅 Setting up scheduler for ${EXPORT_NAME} to run at 1:00 PM IST..."
    
    # Delete the scheduler if it already exists
    if gcloud scheduler jobs describe ${SCHEDULER_ID} --location=${REGION} &> /dev/null; then
        gcloud scheduler jobs delete ${SCHEDULER_ID} --location=${REGION} --quiet
    fi
    
    # Create the Cloud Scheduler job to publish to Pub/Sub with IST timezone
    gcloud scheduler jobs create pubsub ${SCHEDULER_ID} \
        --location=${REGION} \
        --schedule="${SCHEDULE}" \
        --topic=${TOPIC_NAME} \
        --message-body="{\"export_name\":\"${EXPORT_NAME}\"}" \
        --description="${SCHEDULER_DESC}" \
        --time-zone="Asia/Kolkata" \
        --attributes="origin=scheduler"
        
    echo "✅ Scheduler created for ${EXPORT_NAME}"
done

echo "🎉 All schedulers have been set up successfully!"