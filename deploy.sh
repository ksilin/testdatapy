#!/bin/bash

# Deploy testdatapy to Kubernetes

# Set namespace
NAMESPACE=confluent

echo "Building and pushing Docker image..."
docker buildx build . --platform linux/amd64 -t ghcr.io/ksilin/testdatapy:0.0.1 --push

echo "Deploying to Kubernetes..."

# Apply ConfigMap (can be updated)
kubectl apply -f k8s/configmap.yaml -n $NAMESPACE

# Delete existing job if it exists
if kubectl get job testdatapy-producer -n $NAMESPACE >/dev/null 2>&1; then
    echo "Deleting existing job..."
    kubectl delete job testdatapy-producer -n $NAMESPACE
    # Wait for deletion
    kubectl wait --for=delete job/testdatapy-producer -n $NAMESPACE --timeout=30s
fi

# Run a new job
echo "Creating new job..."
kubectl apply -f k8s/job.yaml -n $NAMESPACE

echo "Deployment complete!"
echo ""
echo "To check job status:"
echo "  kubectl get jobs -n $NAMESPACE"
echo "  kubectl get pods -n $NAMESPACE"
echo ""
echo "To check logs:"
echo "  kubectl logs -l app=testdatapy -n $NAMESPACE --tail=50"
echo ""
echo "To follow logs:"
echo "  kubectl logs -l app=testdatapy -n $NAMESPACE -f"
echo ""
echo "To delete the job:"
echo "  kubectl delete job testdatapy-producer -n $NAMESPACE"
echo ""
echo "To deploy the CronJob for continuous generation:"
echo "  kubectl apply -f k8s/cronjob.yaml -n $NAMESPACE"
