"""
Example Kubernetes Deployment

This example shows how to deploy an application using pglease for coordination
across multiple pods.
"""

# deployment.yaml
DEPLOYMENT_YAML = """
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
data:
  DATABASE_URL: "postgresql://user:password@postgres-service:5432/mydb"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worker-app
spec:
  replicas: 3  # Multiple pods running the same code
  selector:
    matchLabels:
      app: worker
  template:
    metadata:
      labels:
        app: worker
    spec:
      containers:
      - name: worker
        image: myapp:latest
        env:
        - name: DATABASE_URL
          valueFrom:
            configMapKeyRef:
              name: app-config
              key: DATABASE_URL
        - name: HOSTNAME  # Kubernetes automatically sets this to pod name
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
"""

# Save deployment configuration
with open("k8s-deployment.yaml", "w") as f:
    f.write(DEPLOYMENT_YAML)

print("Created k8s-deployment.yaml")
print("\nDeploy with:")
print("  kubectl apply -f k8s-deployment.yaml")
print("\nVerify pods are running:")
print("  kubectl get pods -l app=worker")
print("\nCheck logs to see coordination in action:")
print("  kubectl logs -f deployment/worker-app")
