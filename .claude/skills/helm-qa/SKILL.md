---
name: helm-qa
description: QA the Logflare Helm chart in `helm/` and verify what a running pod actually does. Use when a change touches `helm/`, `rel/vm.args.eex`, `rel/env.sh.eex`, `run.sh`, or anything a pod reads at boot (env, ConfigMaps, mounts, VM flags), or when asked to prove a chart change on a real pod. Covers static checks, a local k3s or kind cluster, a before/after comparison against the base chart, and in-pod checks with `logflare rpc`.
---

# Helm QA

A rendered template proves only what Kubernetes receives. It does not prove what the BEAM
booted with. This skill checks both: first the chart statically, then a real pod
installed from the chart, compared with a pod installed from the base branch's chart.

The steps:

1. [Static checks](#1-static-checks): lint, unit tests and a render matrix. No cluster needed.
2. [Cluster and image](#2-cluster-and-image): a local cluster and an image built from the branch.
3. [Install base and change](#3-install-the-base-and-the-change): the same values on both charts.
4. [Verify the pods](#4-verify-the-pods): env, mounts, VM flags and runtime values.
5. [Upgrade in place](#5-upgrade-in-place): `helm upgrade` from the base release to the change.

## 1. Static checks

CI runs these in `.github/workflows/helm-chart.yml`. Run them first: they take seconds.

```sh
# Once. Helm 4 needs --verify=false on both installs; Helm 3 rejects the flag.
helm plugin install https://github.com/helm-unittest/helm-unittest
helm plugin install https://github.com/losisin/helm-values-schema-json

helm lint helm
helm unittest -f '*_test.yaml' helm
(cd helm && helm schema) && git diff --exit-code helm/values.schema.json
```

`helm schema` regenerates `values.schema.json` from `values.yaml`. CI fails when the committed file differs, so commit the regenerated file.

If `helm` is missing, download a release from `https://get.helm.sh/helm-v3.19.0-linux-amd64.tar.gz`.
Older Helm builds (before about 3.17) cannot load the current helm-unittest plugin, which fails with `unknown field "platformHooks"`.

Then render every combination of the values that the changed templates branch on.
A dangling `{{- if }}`, or a `volumes:` key with an empty list, renders only for some combinations.

```sh
for args in "" "--set logflare.certFilesSecret=certs"; do
  echo "== deployment $args"
  helm template t helm $args | kubectl apply --dry-run=client -f - >/dev/null && echo ok
  echo "== rollout $args"
  helm template t helm $args --set rollout.enabled=true \
    --set 'rollout.strategy.canary.steps[0].setWeight=100' \
    | python3 -c 'import sys,yaml; list(yaml.safe_load_all(sys.stdin))' && echo ok
done
```

Add a combination for each value that the changed templates test with `if` or `with`.
`kubectl apply --dry-run=client` needs a reachable cluster. It catches invalid YAML and wrong field types.
The Rollout kind needs the Argo Rollouts CRD, so only parse its YAML. A Rollout also needs a `strategy`, or the chart refuses to render.

When you remove a value, a template or a file:

- `grep -rn` the repo for its name, including `helm/README.md`, `docs/` and the `*_test.yaml` `templates:` lists. helm-unittest fails on a missing template file.
- Check `values.schema.json`. With no `additionalProperties: false`, old values files that still set the key keep working. Say so in the PR.
- Bump `version` in `helm/Chart.yaml`. Bump the minor version when you remove or rename a value.

## 2. Cluster and image

### Image

Build the branch's image as in Procedure A1 of the `ui-qa` skill. In a cloud session, also follow its
[Builds behind a TLS proxy](../ui-qa/SKILL.md#builds-behind-a-tls-proxy) section.

If the change touches only the release (`rel/`, `config/runtime.exs`, `lib/`) and the runner stage fails for reasons unrelated to it, build only the builder stage. Then copy its release into the published image of the same version:

```dockerfile
# Append to a scratch copy of the Dockerfile, below the builder stage, in place of the runner stage.
FROM mirror.gcr.io/supabase/logflare:<VERSION>
RUN rm -rf /opt/app/rel/logflare
COPY --from=builder /app/_build/prod/rel/logflare /opt/app/rel/logflare
COPY --from=builder /app/VERSION /opt/app/VERSION
WORKDIR /opt/app/rel/logflare/bin
COPY run.sh ./run.sh
CMD ["sh", "run.sh"]
```

Say in your report that you did this, and why. Never remove a checksum or other integrity check to get a build through.

Check that the image carries the change before you deploy it:

```sh
docker run --rm --entrypoint cat logflare:qa /opt/app/rel/logflare/releases/<VERSION>/vm.args | grep -v '^#' | grep .
```

### Cluster

Use whatever runs in the environment:

- **kind**, where Docker supports nested containers: `kind create cluster --name lfqa`, then `kind load docker-image logflare:qa --name lfqa`.
- **k3s on the host with the Docker runtime**, where kind fails (as in Claude Code cloud sessions). The cluster shares the Docker daemon, so local images need no loading:

  ```sh
  NO_PROXY="$NO_PROXY,<node-ip>" no_proxy="$NO_PROXY,<node-ip>" \
    k3s server --docker --disable traefik,metrics-server --write-kubeconfig-mode 644 \
    > /tmp/k3s.log 2>&1   # run as a background task
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml NO_PROXY="$NO_PROXY,<node-ip>"
  ```

  `<node-ip>` is the `INTERNAL-IP` from `kubectl get nodes -o wide`. Start k3s once to learn it, then restart it with `NO_PROXY` set. See [Pitfalls](#pitfalls) for the two fixes that cloud sessions need.

Before you install Logflare, check that the cluster runs pods: `kubectl run t --image=mirror.gcr.io/library/busybox --restart=Never -- echo ok`, then `kubectl logs t`.

### Dependencies

Single-tenant mode with the Postgres backend needs no cloud credentials. Run Postgres in the cluster:

```sh
kubectl create deployment pg --image=mirror.gcr.io/library/postgres:16 --port=5432 \
  -- docker-entrypoint.sh postgres -c wal_level=logical
kubectl set env deployment/pg POSTGRES_PASSWORD=postgres POSTGRES_DB=logflare
kubectl expose deployment pg --port=5432
```

The chart never creates a Secret. Create one with throwaway values:

```sh
kubectl create secret generic logflare-secrets \
  --from-literal=DB_PASSWORD=postgres \
  --from-literal=POSTGRES_BACKEND_URL=postgresql://postgres:postgres@pg.default.svc.cluster.local:5432/logflare \
  --from-literal=LOGFLARE_PUBLIC_ACCESS_TOKEN=$(openssl rand -hex 16) \
  --from-literal=LOGFLARE_PRIVATE_ACCESS_TOKEN=$(openssl rand -hex 16) \
  --from-literal=LOGFLARE_DB_ENCRYPTION_KEY=$(openssl rand -base64 32) \
  --from-literal=PHX_SECRET_KEY_BASE=$(openssl rand -base64 48) \
  --from-literal=PHX_LIVE_VIEW_SIGNING_SALT=$(openssl rand -hex 8)
```

## 3. Install the base and the change

Write one values file that mirrors how production sets the chart. k8s-declarative's
`charts/LogflareChart.values.ts` is the reference. Set at least the values that the change touches.
For example, production passes `ERL_FLAGS` through `extraConfig`:

```yaml
# qa-values.yaml
image: {repository: logflare, tag: qa, pullPolicy: Never}
logflare:
  secretRefs: [logflare-secrets]
  db: {hostname: pg.default.svc.cluster.local}
  backend: {type: postgres}
  extraConfig:
    ERL_FLAGS: "+S 2:2"
```

Install the base chart with the published image in its own namespace. This gives you the "before"
numbers to compare with, and proves that the check can tell a broken pod from a fixed one:

```sh
mkdir -p /tmp/basechart && git archive origin/main helm | tar -x -C /tmp/basechart
kubectl create ns base
kubectl get secret logflare-secrets -o json | jq '.metadata |= {name}' | kubectl -n base apply -f -
helm install lf /tmp/basechart/helm -n base -f qa-values.yaml \
  --set image.repository=mirror.gcr.io/supabase/logflare --set image.tag=<VERSION> --set image.pullPolicy=IfNotPresent
```

Then install the change:

```sh
helm install lf helm -f qa-values.yaml
kubectl rollout status deploy/lf-logflare --timeout=5m
```

Both point at the same database. Uninstall the base release before you install the change if the change adds migrations.

If a pod does not become ready, read `kubectl describe pod` and `kubectl logs`. A `GenSingleton` warning about `wal_level` means Postgres runs without `-c wal_level=logical`. It does not stop the app.

## 4. Verify the pods

Run each check on the base pod and the changed pod. Report both values.

**What Kubernetes gave the container:**

```sh
# The newest pod. Right after a rollout, the old pod can still be terminating.
P=$(kubectl get pod -l app.kubernetes.io/name=logflare --sort-by=.metadata.creationTimestamp -o name | tail -1)
kubectl exec $P -- env | sort | grep -E '^(ERL_|ELIXIR_|RELEASE_)'  # never print the Secret's values
kubectl get $P -o jsonpath='{.spec.volumes}{"\n"}{.spec.containers[0].volumeMounts}{"\n"}'
kubectl get cm
```

**What the VM booted with.** The image has no `pgrep` or `ps`, so scan `/proc`:

```sh
kubectl exec -i $P -- sh <<'EOF'
for d in /proc/[0-9]*; do c=$(tr '\0' ' ' < $d/cmdline 2>/dev/null)
  case "$c" in *beam.smp*) echo "$c"; break;; esac; done
EOF
```

Flags from `vm.args` come first, then `ERL_FLAGS` and `ELIXIR_ERL_OPTIONS`. A later flag overrides an earlier one, so an `ERL_FLAGS` `+S` must come after any `+S` from the release.

**Runtime values**, through the release's remote shell:

```sh
kubectl exec $P -- /opt/app/rel/logflare/bin/logflare rpc 'IO.inspect(%{
  dist_buf_busy_limit: :erlang.system_info(:dist_buf_busy_limit),
  schedulers_online: :erlang.system_info(:schedulers_online),
  port_limit: :erlang.system_info(:port_limit),
  process_limit: :erlang.system_info(:process_limit),
  atom_limit: :erlang.system_info(:atom_limit),
  ets_limit: :erlang.system_info(:ets_limit),
  net_ticktime: :net_kernel.get_net_ticktime()
})'
```

Map each flag to the value it sets, and check the value, not just the flag:

| Flag | Check | Note |
|---|---|---|
| `+zdbbl N` | `:dist_buf_busy_limit` | in bytes: `N * 1024` |
| `+S a:b` | `:schedulers_online` | |
| `+Q N` | `:port_limit` | rounded up to a power of two |
| `+P N` | `:process_limit` | rounded up to a power of two |
| `+t N` | `:atom_limit` | |
| `-env ERL_MAX_ETS_TABLES N` | `:ets_limit` | |
| `-kernel net_ticktime N` | `:net_kernel.get_net_ticktime()` | |
| `+sbwt`, `+swt`, `+sub`, `+Mdai`, `+IOt` | `beam.smp` command line only | no `system_info` key |

For changes that are not VM flags, use the same pattern: find the value that the pod reads at boot and
read it back from the running node with `logflare rpc`, such as `Application.get_env/2` for runtime config.

## 5. Upgrade in place

Production upgrades a running release. It does not install a new one. Upgrade the base release to the changed chart and check that the rollout replaces the pod and that the old objects are gone:

```sh
helm upgrade lf helm -n base -f qa-values.yaml --set image.tag=qa --set image.repository=logflare --set image.pullPolicy=Never
kubectl -n base rollout status deploy/lf-logflare --timeout=5m
kubectl -n base get cm,secret
```

Then run the checks in step 4 on the new pod.

### The chart's default image

Users who upgrade the chart without pinning `image.tag` get `.Chart.AppVersion`, the last released image.
It was built before your change. If the change moves something from the chart into the image, such as a
VM flag moved from a chart value into `rel/vm.args.eex`, that image does not have it yet. Upgrade once with the default tag:

```sh
helm upgrade lf helm -n base -f qa-values.yaml \
  --set image.repository=mirror.gcr.io/supabase/logflare --set image.tag="" --set image.pullPolicy=IfNotPresent
```

Run step 4 again. Anything that the base pod had and this pod lacks is a regression for every upgrader until the next release ships.
Fix it in the PR, or state in the PR the minimum image version that the chart needs.

## Report the result

Give the reviewer:

- The commit tested and how you built the image (full build, or builder stage on the published runner, and why).
- The static checks you ran and their results.
- A before/after table of each check in step 4, including the default-image upgrade.
- Anything you could not verify, and why.

## Clean up

```sh
helm uninstall lf; helm uninstall lf -n base; kubectl delete ns base
kubectl delete deploy/pg svc/pg secret/logflare-secrets
kind delete cluster --name lfqa   # or stop the k3s background task
```

## Pitfalls

- **`runc` fails with `failed to update /proc/self/oom_score_adj: Permission denied`.** The sandbox forbids a negative `oom_score_adj`, and kubelet sets one for each pod sandbox. Put a wrapper in front of `runc` that clamps `process.oomScoreAdj` to `0` in the bundle's `config.json`, register it in `/etc/docker/daemon.json` as `{"runtimes":{"clamp":{"path":"/usr/local/bin/runc-clamp"}},"default-runtime":"clamp"}`, and run `pkill -HUP dockerd`. Docker reloads runtimes without a restart, so running builds survive. This is for the test sandbox only.
- **`kubectl logs` and `exec` fail with `EOF` and an agent-proxy `ws_closed_mid_exchange` warning.** The API server sends kubelet traffic through `HTTPS_PROXY`. Start k3s with the node IP in `NO_PROXY`, and set the same `NO_PROXY` for `kubectl`.
- **`kind create cluster` fails at `wait-control-plane` with `can't get final child's PID from pipe`.** Nested containers do not work in the sandbox. Use k3s on the host.
- **`pkill -f "k3s server"` kills your own shell.** The pattern matches the shell command that contains it. Run k3s as a background task and stop that task, or kill it by PID.
- **The base and changed releases share object names.** Install them in separate namespaces.
- **A Secret copied to another namespace keeps short service names.** `pg` resolves only in its own namespace, so use `pg.default.svc.cluster.local` in every URL.
- **`helm install` with `pullPolicy: Never` gives `ErrImageNeverPull` on kind.** Load the image with `kind load docker-image` first.
