#!/bin/bash
set -e

APP_DIR="/home/ubuntu/app"
ENV_FILE="${APP_DIR}/.env"
REGION="ap-northeast-2"
SSM_PREFIX="/psycho/prod"

echo "========================================"
echo " AfterInstall: $(date)"
echo "========================================"

get_param() {
  aws ssm get-parameter \
    --name "${SSM_PREFIX}/$1" \
    --with-decryption \
    --query Parameter.Value \
    --output text \
    --region ${REGION}
}

echo "Loading environment variables from SSM..."

cat > ${ENV_FILE} <<EOF
DB_HOST=$(get_param db-host)
DB_PORT=$(get_param db-port)
DB_NAME=$(get_param db-name)
DB_USERNAME=$(get_param db-username)
DB_PASSWORD=$(get_param db-password)
OPENAI_API_KEY=$(get_param openai-api-key)
OPENAI_WEBHOOK_SECRET=$(get_param openai-webhook-secret)
SQS_QUEUE_URL=$(get_param sqs-queue-url)
BUSINESS_NOTIFY_SQS_QUEUE_URL=$(get_param business-notify-sqs-queue-url)
AWS_REGION=$(get_param aws-region)
ENABLE_INPROCESS_WORKER=true
DB_AUTO_CREATE_TABLES=true
EOF

chmod 600 ${ENV_FILE}
chown ubuntu:ubuntu ${ENV_FILE}

echo "Environment file created: ${ENV_FILE}"

echo "Installing dependencies..."
cd ${APP_DIR}
sudo -u ubuntu /home/ubuntu/.local/bin/uv sync --frozen

echo "Installing systemd service..."
cp ${APP_DIR}/pickle.service /etc/systemd/system/pickle.service
chmod 644 /etc/systemd/system/pickle.service
systemctl daemon-reload
systemctl enable pickle

echo "AfterInstall completed."
