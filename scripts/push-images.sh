#!/usr/bin/env bash
# Build and push both images to Amazon ECR.
# Usage: AWS_REGION=ap-south-1 ECR_REGISTRY=<account>.dkr.ecr.<region>.amazonaws.com IMAGE_TAG=v1 ./scripts/push-images.sh
set -euo pipefail

: "${ECR_REGISTRY:?Set ECR_REGISTRY, e.g. 123456789012.dkr.ecr.ap-south-1.amazonaws.com}"
AWS_REGION="${AWS_REGION:-ap-south-1}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

for repo in kafka-obs-app kafka-obs-kafka; do
  aws ecr describe-repositories --repository-names "$repo" --region "$AWS_REGION" >/dev/null 2>&1 \
    || aws ecr create-repository --repository-name "$repo" --region "$AWS_REGION" \
         --image-scanning-configuration scanOnPush=true >/dev/null
done

aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_REGISTRY"

docker build -t "$ECR_REGISTRY/kafka-obs-app:$IMAGE_TAG" .
docker build -t "$ECR_REGISTRY/kafka-obs-kafka:$IMAGE_TAG" ./kafka
docker push "$ECR_REGISTRY/kafka-obs-app:$IMAGE_TAG"
docker push "$ECR_REGISTRY/kafka-obs-kafka:$IMAGE_TAG"
echo "Pushed images with tag $IMAGE_TAG"
