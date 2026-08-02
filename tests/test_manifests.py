from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]


def test_kubernetes_manifests_are_valid_yaml():
    for path in (
        ROOT / "kubernetes" / "namespace.yaml",
        ROOT / "kubernetes" / "spark-rbac.yaml",
    ):
        documents = list(yaml.safe_load_all(path.read_text()))
        assert documents
        assert all(
            document.get("apiVersion") and document.get("kind")
            for document in documents
        )


def test_helm_values_are_valid_yaml():
    values = yaml.safe_load(
        (ROOT / "kubernetes" / "airflow_helm_values.yaml").read_text()
    )

    assert values["executor"] == "KubernetesExecutor"
    assert values["postgresql"]["enabled"] is True
    assert "pass" not in values.get("data", {}).get("metadataConnection", {})
