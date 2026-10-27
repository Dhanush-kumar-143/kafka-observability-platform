#!/usr/bin/env bash
# Deploy the whole stack to the cluster kubectl is currently pointed at.
# Usage: ECR_REGISTRY=<registry> IMAGE_TAG=v1 ./scripts/deploy-eks.sh
set -euo pipefail

: "${ECR_REGISTRY:?Set ECR_REGISTRY, e.g. 123456789012.dkr.ecr.ap-south-1.amazonaws.com}"
export ECR_REGISTRY
export IMAGE_TAG="${IMAGE_TAG:-latest}"
NS=kafka-obs

cd "$(dirname "$0")/.."

kubectl apply -f k8s/00-namespace.yaml

# Config files become ConfigMaps (same files docker-compose mounts)
kubectl -n "$NS" create configmap prometheus-config \
  --from-file=monitoring/prometheus/prometheus.yml \
  --from-file=monitoring/prometheus/alert_rules.yml \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create configmap alertmanager-config \
  --from-file=alertmanager.yml=monitoring/alertmanager/alertmanager.yml \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create configmap grafana-datasources \
  --from-file=monitoring/grafana/provisioning/datasources/datasource.yml \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create configmap grafana-dashboard-provider \
  --from-file=monitoring/grafana/provisioning/dashboards/dashboards.yml \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create configmap grafana-dashboards \
  --from-file=monitoring/grafana/dashboards/kafka-observability.json \
  --dry-run=client -o yaml | kubectl apply -f -

# Grafana admin credentials (created once, never overwritten)
if ! kubectl -n "$NS" get secret grafana-admin >/dev/null 2>&1; then
  GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-$(openssl rand -base64 18)}"
  kubectl -n "$NS" create secret generic grafana-admin \
    --from-literal=user=admin --from-literal=password="$GRAFANA_ADMIN_PASSWORD"
  echo "Grafana admin password: $GRAFANA_ADMIN_PASSWORD"
fi

# Manifests that reference the images get ${ECR_REGISTRY} / ${IMAGE_TAG} substituted
for f in k8s/kafka.yaml k8s/app.yaml; do
  envsubst '${ECR_REGISTRY} ${IMAGE_TAG}' < "$f" | kubectl apply -f -
done
for f in k8s/kafka-exporter.yaml k8s/prometheus.yaml k8s/alertmanager.yaml k8s/grafana.yaml; do
  kubectl apply -f "$f"
done

kubectl -n "$NS" rollout status statefulset/kafka --timeout=300s
for d in kafka-exporter producer consumer prometheus alertmanager grafana; do
  kubectl -n "$NS" rollout status deployment/"$d" --timeout=180s
done

echo
echo "Deployed. Open Grafana with:"
echo "  kubectl -n $NS port-forward svc/grafana 3000:3000"
