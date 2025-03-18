#!/bin/bash
set -e

# Configuration
PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"insightsprod"}
REGION=${REGION:-"us-central1"}
SERVICE_NAME="boxout-bq-sftp-export"
REPOSITORY_NAME="sftp-export"
IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY_NAME}/${SERVICE_NAME}"

echo "🛠️ Deploying BigQuery SFTP Export to Cloud Run"
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Service: ${SERVICE_NAME}"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "❌ gcloud CLI not found. Please install it first."
    exit 1
fi

# Check if logged in
echo "🔑 Checking authentication..."
ACCOUNT=$(gcloud config get-value account 2>/dev/null)
if [ -z "$ACCOUNT" ]; then
    echo "❌ Not logged in. Please run 'gcloud auth login' first."
    exit 1
fi

# Build the Docker image with explicit platform for Cloud Run
echo "🔨 Building Docker image for AMD64 architecture..."
docker build --platform linux/amd64 -t ${IMAGE_NAME} .

# Push the image to Google Container Registry
echo "📤 Pushing image to Google Container Registry..."
docker push ${IMAGE_NAME}

# Deploy to Cloud Run
echo "🚀 Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
  --image ${IMAGE_NAME} \
  --platform managed \
  --region ${REGION} \
  --memory 2Gi \
  --timeout 900s \
  --no-allow-unauthenticated \
  --set-env-vars="EXPORT_METADATA_TABLE=${EXPORT_METADATA_TABLE:-insightsprod.boxoutllc_sftp_metadata.export_metadata}" \
  --set-env-vars="GCS_BUCKET=${GCS_BUCKET:-boxout-sftp-transfer}" \
  --set-env-vars="SFTP_HOST=${SFTP_HOST:-sftp.boxouthealth.com}" \
  --set-env-vars="SFTP_USERNAME=${SFTP_USERNAME:-saras}" \
  --set-env-vars="SFTP_PASSWORD=${SFTP_PASSWORD:-1pEphpq1}" \
  --set-env-vars="SFTP_DIRECTORY=${SFTP_DIRECTORY:-/saras}" \
  --set-env-vars="PYTHONUNBUFFERED=1" \
  --cpu=1 \
  --concurrency=1 \
  --min-instances=0 \
  --max-instances=10

echo "✅ Deployment completed successfully!"
echo "🌐 Service URL: $(gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format='value(status.url)')"