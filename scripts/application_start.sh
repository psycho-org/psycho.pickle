#!/bin/bash
set -e

SERVICE_NAME="pickle"

echo "========================================"
echo " ApplicationStart: $(date)"
echo "========================================"

systemctl daemon-reload
systemctl start ${SERVICE_NAME}

echo "ApplicationStart completed."