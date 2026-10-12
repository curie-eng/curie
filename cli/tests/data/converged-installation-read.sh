# Common external-process replies for tests focused on pre-installation
# inference/credentials. The convergence-specific consumer tests independently
# exercise failure/secondary paths; these callers need a healthy target after
# their existing successful Helm mutation. Do not answer helm history here:
# this file is prepended, and a shared not-found reply would shadow fixtures
# that classify history (gVisor retry, failed-only discard). Callers answer
# history in their own helm fake. No earlier mutation/preflight reply or
# assertion is changed here.
# Opted-in install fixtures answer the additional advisory reads without
# replacing fixtures that exercise their own node/pod behavior. Shapes follow
# https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/node-v1/
# and https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/.
curie_test_capacity_read() {
    case "${0##*/}:$1:${2:-}" in
        kubectl:get:nodes)
            printf '%s\n' '{"apiVersion":"v1","kind":"NodeList","items":[{"metadata":{"name":"acme-capacity-node"},"spec":{},"status":{"allocatable":{"cpu":"64","memory":"128Gi"},"conditions":[{"type":"Ready","status":"True"}]}}]}'
            exit 0
            ;;
        kubectl:get:pods)
            case " $* " in
                *" --all-namespaces "*)
                    printf '%s\n' '{"apiVersion":"v1","kind":"PodList","items":[]}'
                    exit 0
                    ;;
            esac
            ;;
    esac
    return 1
}
case "${0##*/}:$1:${2:-}" in
    helm:status:*)
        if [ -f "${0%/*}/convergence-release-failed" ]; then
            printf '%s\n' '{"version":1,"info":{"status":"failed"},"hooks":[]}'
            exit 0
        fi
        printf '%s\n' '{"version":1,"info":{"status":"deployed"},"hooks":[]}'
        exit 0
        ;;
    helm:get:manifest)
        cat <<'JSON'
{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"acme-probe"},"spec":{"replicas":1,"selector":{"matchLabels":{"component":"acme-probe"}},"template":{"spec":{"containers":[{"name":"probe","image":"busybox:1"}]}}}}
JSON
        exit 0
        ;;
    kubectl:get:deployments,statefulsets,daemonsets,pods,jobs)
        cat <<'JSON'
{"items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"acme-probe","generation":1},"spec":{"replicas":1,"selector":{"matchLabels":{"component":"acme-probe"}},"template":{"spec":{"containers":[{"name":"probe","image":"busybox:1"}]}}},"status":{"observedGeneration":1,"replicas":1,"updatedReplicas":1,"readyReplicas":1}},{"kind":"Pod","metadata":{"name":"acme-probe-pod","labels":{"component":"acme-probe"}},"spec":{"containers":[{"name":"probe","image":"busybox:1"}]},"status":{"phase":"Running","containerStatuses":[{"name":"probe","image":"busybox:1","imageID":"containerd://sha256:example","ready":true,"state":{"running":{}}}]}}]}
JSON
        exit 0
        ;;
esac
