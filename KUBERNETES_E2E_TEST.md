# End-to-End Testing with Kubernetes (sdv-runtime pod)

## 🎯 Your Setup

You mentioned: **"the kuksa-databroker is hosted inside of sdv-runtime pod"**

This guide shows how to test `setCurrentValue()` between your C++ client and the kuksa-client CLI when the databroker is running in a Kubernetes pod.

---

## 📋 Prerequisites

1. ✅ sdv-runtime pod is running with kuksa-databroker
2. ✅ Databroker port (55555) is accessible
3. ✅ Your C++ client image is built: `kuksa-databroker-client:amd64`

---

## 🔍 Step 1: Find Your Databroker

First, locate the databroker in your Kubernetes cluster:

```bash
# Find the sdv-runtime pod
kubectl get pods | grep sdv-runtime

# Get detailed pod info
kubectl describe pod <sdv-runtime-pod-name>

# Check if databroker is running inside
kubectl exec <sdv-runtime-pod-name> -- ps aux | grep databroker

# Check which port databroker is listening on
kubectl exec <sdv-runtime-pod-name> -- netstat -tlnp | grep 55555
```

**Expected output:**
```
NAME                          READY   STATUS    RESTARTS   AGE
sdv-runtime-xxxxx-yyyyy       1/1     Running   0          2h
```

---

## 🌐 Step 2: Setup Port Forwarding

Make the databroker accessible from your local machine:

```bash
# Forward port 55555 from the pod to your localhost
kubectl port-forward pod/<sdv-runtime-pod-name> 55555:55555
```

**Keep this terminal open!** This creates a tunnel:
```
Your Machine:55555 → Pod:55555 → Databroker
```

**Verify it works:**
```bash
# In another terminal:
curl http://localhost:55555 || echo "Port forwarding active"
netstat -an | grep 55555
```

---

## 🚀 Step 3: Test C++ Client → Databroker

### Option A: Run C++ Client on Host Network

```bash
# Terminal 2 (port-forward running in Terminal 1)
docker run --rm --network host \
  kuksa-databroker-client:amd64 \
  /usr/local/bin/KuksaDatabrokerClient 127.0.0.1:55555
```

**What should happen:**
```
╔════════════════════════════════════════╗
║  VSS Signal End-to-End Test Suite     ║
╚════════════════════════════════════════╝

Connecting to KUKSA Databroker at 127.0.0.1:55555...
✓ Connected successfully!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  SENSOR SIGNALS (End-to-End Testing)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  → Testing Sensor: Vehicle.Speed
    ✓ Write 0.0 → Read 0.0 (45.23ms)
    ✓ Write 60.5 → Read 60.5 (42.15ms)
...
```

### Option B: Run C++ Client Inside Kubernetes

Create a test pod that can access the databroker directly:

```yaml
# test-client-pod.yaml
apiVersion: v1
kind: Pod
metadata:
  name: kuksa-test-client
spec:
  containers:
  - name: test-client
    image: kuksa-databroker-client:amd64
    command: ["/bin/bash", "-c", "sleep 3600"]
  restartPolicy: Never
```

Deploy and run:
```bash
# Create the pod
kubectl apply -f test-client-pod.yaml

# Wait for it to start
kubectl wait --for=condition=Ready pod/kuksa-test-client

# Get the databroker service IP
DATABROKER_IP=$(kubectl get pod <sdv-runtime-pod-name> -o jsonpath='{.status.podIP}')

# Run the test
kubectl exec kuksa-test-client -- \
  /usr/local/bin/KuksaDatabrokerClient ${DATABROKER_IP}:55555
```

---

## 🔄 Step 4: Verify with kuksa-client CLI

### Option A: CLI via Port Forward (Host)

```bash
# Terminal 3 (port-forward still running in Terminal 1)
docker run -it --rm --network host \
  ghcr.io/eclipse/kuksa.val/databroker:master \
  kuksa-client --server 127.0.0.1:55555

# In the CLI:
getValue Vehicle.Speed
getValue Vehicle.Chassis.Accelerator.PedalPosition
```

### Option B: CLI Inside Kubernetes

```bash
# Run kuksa-client inside the sdv-runtime pod
kubectl exec -it <sdv-runtime-pod-name> -- kuksa-client --server localhost:55555

# Or if kuksa-client isn't in the pod, use a separate pod:
kubectl run kuksa-cli-test --rm -it --restart=Never \
  --image=ghcr.io/eclipse/kuksa.val/databroker:master -- \
  kuksa-client --server <sdv-runtime-pod-ip>:55555
```

**In the CLI, verify:**
```bash
# Read values written by C++ client
getValue Vehicle.Speed
getValue Vehicle.Chassis.Accelerator.PedalPosition

# Expected:
{
  "path": "Vehicle.Speed",
  "value": {
    "value": 50.0,
    "timestamp": "..."
  }
}
```

---

## 🔄 Step 5: Bidirectional Test

### Test 1: CLI Write → C++ Read

**Terminal 3 (kuksa-client CLI):**
```bash
setValue Vehicle.Speed 99.9
getValue Vehicle.Speed
```

**Terminal 2 (run C++ test again):**
```bash
docker run --rm --network host \
  kuksa-databroker-client:amd64 \
  /usr/local/bin/KuksaDatabrokerClient 127.0.0.1:55555
```

The C++ test should read the value you just set (99.9) or overwrite it with test values.

### Test 2: C++ Write → CLI Read

**Terminal 2 (C++ running):**
The test will write various values to signals.

**Terminal 3 (CLI):**
```bash
# Subscribe to see updates
subscribe Vehicle.Speed

# You should see:
Subscription update:
{
  "path": "Vehicle.Speed",
  "value": 60.5
}
Subscription update:
{
  "path": "Vehicle.Speed",
  "value": 120.0
}
```

---

## 📊 Step 6: Run Automated Tests with Kubernetes

Modify the test scripts to use port-forwarding:

```bash
# Terminal 1: Keep port-forward running
kubectl port-forward pod/<sdv-runtime-pod-name> 55555:55555 &
PF_PID=$!

# Wait for port-forward to establish
sleep 2

# Terminal 2: Run tests using localhost
docker run --rm --network host \
  kuksa-databroker-client:amd64 \
  /usr/local/bin/KuksaDatabrokerClient 127.0.0.1:55555

# Verify with CLI
docker run -it --rm --network host \
  ghcr.io/eclipse/kuksa.val/databroker:master \
  kuksa-client --server 127.0.0.1:55555 <<EOF
getValue Vehicle.Speed
quit
EOF

# Cleanup
kill $PF_PID
```

---

## 🛠️ Kubernetes-Specific Test Script

Create a specialized test script:

```bash
#!/bin/bash
# k8s_e2e_test.sh - E2E test for Kubernetes deployment

set -e

# Configuration
POD_NAME=${1:-$(kubectl get pods -l app=sdv-runtime -o jsonpath='{.items[0].metadata.name}')}
LOCAL_PORT=55555
POD_PORT=55555

echo "Testing with pod: $POD_NAME"

# Start port forward
echo "Starting port forward..."
kubectl port-forward pod/$POD_NAME $LOCAL_PORT:$POD_PORT &
PF_PID=$!

# Wait for port forward to be ready
sleep 3

# Cleanup function
cleanup() {
    echo "Stopping port forward..."
    kill $PF_PID 2>/dev/null || true
}
trap cleanup EXIT

# Run C++ test
echo "Running C++ test suite..."
docker run --rm --network host \
    kuksa-databroker-client:amd64 \
    /usr/local/bin/KuksaDatabrokerClient 127.0.0.1:$LOCAL_PORT \
    > /tmp/k8s_cpp_test.log 2>&1

# Check results
if grep -q "All tests completed" /tmp/k8s_cpp_test.log; then
    echo "✓ C++ tests PASSED"
    passed=$(grep "Passed:" /tmp/k8s_cpp_test.log | awk '{print $2}')
    echo "  Passed: $passed tests"
else
    echo "✗ C++ tests FAILED"
    cat /tmp/k8s_cpp_test.log
    exit 1
fi

# Verify with CLI
echo "Verifying with kuksa-client CLI..."
docker run --rm --network host \
    ghcr.io/eclipse/kuksa.val/databroker:master \
    kuksa-client --server 127.0.0.1:$LOCAL_PORT <<EOF > /tmp/k8s_cli_test.log 2>&1
getValue Vehicle.Speed
quit
EOF

if grep -q "value" /tmp/k8s_cli_test.log; then
    echo "✓ CLI verification PASSED"
else
    echo "✗ CLI verification FAILED"
    cat /tmp/k8s_cli_test.log
    exit 1
fi

echo "✓✓✓ All tests PASSED ✓✓✓"
```

**Make it executable and run:**
```bash
chmod +x k8s_e2e_test.sh
./k8s_e2e_test.sh [pod-name]
```

---

## 🔍 Troubleshooting Kubernetes Setup

### Issue: Port forward fails

```bash
# Check if pod is running
kubectl get pod <sdv-runtime-pod-name>

# Check pod logs
kubectl logs <sdv-runtime-pod-name>

# Check if databroker is listening
kubectl exec <sdv-runtime-pod-name> -- netstat -tlnp | grep 55555
```

### Issue: Cannot connect to databroker

```bash
# Check if databroker process is running
kubectl exec <sdv-runtime-pod-name> -- ps aux | grep databroker

# Check databroker logs
kubectl logs <sdv-runtime-pod-name> | grep -i databroker

# Try accessing from inside the pod
kubectl exec -it <sdv-runtime-pod-name> -- \
  curl http://localhost:55555 || echo "Databroker not responding"
```

### Issue: Permission denied

```bash
# Check pod security context
kubectl get pod <sdv-runtime-pod-name> -o yaml | grep -A 10 securityContext

# May need to run test pod with same security context
```

---

## 📋 Kubernetes Testing Checklist

- [ ] sdv-runtime pod is running
- [ ] Port forward is established (55555)
- [ ] C++ client can connect via localhost:55555
- [ ] kuksa-client CLI can connect via localhost:55555
- [ ] C++ can write, CLI can read
- [ ] CLI can write, C++ can read
- [ ] Subscriptions work both ways
- [ ] All signal types tested (float, int, bool, string)

---

## 🎯 Quick Command Reference

```bash
# Find your pod
kubectl get pods | grep sdv-runtime

# Port forward
kubectl port-forward pod/<sdv-runtime-pod> 55555:55555

# Run C++ test (in another terminal)
docker run --rm --network host kuksa-databroker-client:amd64 \
  /usr/local/bin/KuksaDatabrokerClient 127.0.0.1:55555

# Run CLI test
docker run -it --rm --network host \
  ghcr.io/eclipse/kuksa.val/databroker:master \
  kuksa-client --server 127.0.0.1:55555

# Check databroker inside pod
kubectl exec <pod> -- ps aux | grep databroker
kubectl exec <pod> -- netstat -tlnp | grep 55555
kubectl logs <pod> | grep -i databroker
```

---

## 🚀 Next Steps

1. **First time:**
   - Setup port forwarding
   - Run manual test (Steps 1-5)
   - Verify everything works

2. **Regular testing:**
   - Use the k8s_e2e_test.sh script
   - Automate with CI/CD

3. **Production:**
   - Deploy test pod in the same namespace
   - Use Kubernetes service discovery
   - No port-forward needed (pods talk directly)

---

## 📊 Expected Results

**Successful test:**
```
Testing with pod: sdv-runtime-abc123
Starting port forward...
Forwarding from 127.0.0.1:55555 -> 55555

Running C++ test suite...
✓ C++ tests PASSED
  Passed: 48 tests

Verifying with kuksa-client CLI...
✓ CLI verification PASSED

✓✓✓ All tests PASSED ✓✓✓
```

**If it fails:**
- Check port forward is running
- Verify databroker is accessible in pod
- Check pod logs for errors
- Ensure network policies allow communication

---

## 💡 Pro Tips

1. **Use Kubernetes Services:**
   If you're running tests inside the cluster, use service discovery instead of port-forward:
   ```bash
   # If there's a service for databroker
   kubectl get svc | grep databroker
   # Use: <service-name>.<namespace>.svc.cluster.local:55555
   ```

2. **Persistent Testing:**
   Create a DaemonSet or Deployment for continuous testing:
   ```yaml
   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: kuksa-test-client
   spec:
     replicas: 1
     template:
       spec:
         containers:
         - name: test-client
           image: kuksa-databroker-client:amd64
           command: ["/usr/local/bin/KuksaDatabrokerClient"]
           args: ["<databroker-service>:55555"]
   ```

3. **Debug Mode:**
   Run tests with debug enabled:
   ```bash
   kubectl exec -it kuksa-test-client -- bash
   # Inside pod:
   /usr/local/bin/KuksaDatabrokerClient <databroker-ip>:55555
   ```

---

Your setup is now ready for testing with the Kubernetes-hosted databroker! 🎉
