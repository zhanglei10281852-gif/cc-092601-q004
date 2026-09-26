from __future__ import annotations

from datetime import UTC, datetime

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection, transaction


TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "tolerance": {"type": "number", "required": False, "minimum": 0.0, "maximum": 1.0},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {"tolerance": 0.001},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def submit_payload(key: str, *, user: str = "researcher-1", priority: int = 50) -> dict:
    return {
        "template_code": "solver-a",
        "project_code": "project-a",
        "requested_by": user,
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": priority,
        "idempotency_key": key,
    }


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def test_template_submission_idempotency_and_parameter_validation(client):
    create_template(client)
    first = client.post("/api/compute/tasks", json=submit_payload("request-000001"))
    second = client.post("/api/compute/tasks", json=submit_payload("request-000001"))
    assert first.status_code == second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    invalid = submit_payload("request-000002")
    invalid["parameters"]["iterations"] = 20000
    rejected = client.post("/api/compute/tasks", json=invalid)
    assert rejected.status_code == 422


def test_idempotency_scope_isolated_by_project_and_template(client):
    create_template(client)
    second_template = dict(TEMPLATE)
    second_template["code"] = "solver-b"
    assert client.post("/api/compute/templates?actor=administrator", json=second_template).status_code == 201
    key = "shared-portal-key"
    project_a = client.post("/api/compute/tasks", json=submit_payload(key)).json()
    project_b_payload = submit_payload(key)
    project_b_payload["project_code"] = "project-b"
    project_b = client.post("/api/compute/tasks", json=project_b_payload)
    assert project_b.status_code == 202
    assert project_b.json()["id"] != project_a["id"]
    template_b_payload = submit_payload(key)
    template_b_payload["template_code"] = "solver-b"
    template_b = client.post("/api/compute/tasks", json=template_b_payload)
    assert template_b.status_code == 202
    assert template_b.json()["id"] not in {project_a["id"], project_b.json()["id"]}
    repeated = client.post("/api/compute/tasks", json=submit_payload(key))
    assert repeated.status_code == 202
    assert repeated.json()["id"] == project_a["id"]
    tasks = client.get("/api/compute/tasks", params={"project_code": "project-b"}).json()["items"]
    assert [item["project_code"] for item in tasks] == ["project-b"]


def test_same_scope_conflict_explains_parameter_and_priority_differences(client):
    create_template(client)
    first = client.post("/api/compute/tasks", json=submit_payload("conflict-key-01", priority=50))
    assert first.status_code == 202
    different_params = submit_payload("conflict-key-01", priority=50)
    different_params["parameters"]["iterations"] = 200
    params_conflict = client.post("/api/compute/tasks", json=different_params)
    assert params_conflict.status_code == 409
    detail = params_conflict.json()["error"]
    assert "计算参数" in detail["message"]
    assert detail["context"]["existing_task_id"] == first.json()["id"]
    assert detail["context"]["project_code"] == "project-a"
    assert detail["context"]["template_code"] == "solver-a"
    assert "stored_parameter_digest" in detail["context"]
    priority_conflict = client.post("/api/compute/tasks", json=submit_payload("conflict-key-01", priority=80))
    assert priority_conflict.status_code == 409
    priority_detail = priority_conflict.json()["error"]
    assert "优先级" in priority_detail["message"]
    assert priority_detail["context"]["stored_priority"] == 50
    assert priority_detail["context"]["request_priority"] == 80
    both_conflict = submit_payload("conflict-key-01", priority=80)
    both_conflict["parameters"]["iterations"] = 200
    both = client.post("/api/compute/tasks", json=both_conflict).json()["error"]
    assert "计算参数" in both["message"] and "优先级" in both["message"]


def test_repeated_request_does_not_double_deduct_quota(client):
    create_template(client)
    assert client.put(
        "/api/compute/quotas?actor=administrator",
        json={"subject_type": "user", "subject_key": "limited", "max_queued": 10, "max_running": 10, "daily_submissions": 1},
    ).status_code == 200
    payload = submit_payload("quota-repeat-001", user="limited")
    first = client.post("/api/compute/tasks", json=payload)
    assert first.status_code == 202
    repeated = client.post("/api/compute/tasks", json=payload)
    assert repeated.status_code == 202
    assert repeated.json()["id"] == first.json()["id"]
    genuinely_new = client.post("/api/compute/tasks", json=submit_payload("quota-repeat-002", user="limited"))
    assert genuinely_new.status_code == 409


def test_legacy_idempotency_scope_migrates_safely(tmp_path, monkeypatch):
    import json
    import sqlite3

    from app.compute.service import digest
    db_path = tmp_path / "legacy.db"
    now = "2026-09-20T08:00:00+00:00"
    parameters = {"iterations": 100, "mode": "accurate", "tolerance": 0.001}
    raw = sqlite3.connect(db_path)
    raw.executescript(
        """
        CREATE TABLE compute_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
            algorithm TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1,
            parameter_schema_json TEXT NOT NULL, default_parameters_json TEXT NOT NULL DEFAULT '{}',
            max_runtime_seconds INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
            active INTEGER NOT NULL DEFAULT 1, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE compute_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL, project_code TEXT NOT NULL, requested_by TEXT NOT NULL,
            parameters_json TEXT NOT NULL, parameter_digest TEXT NOT NULL,
            priority INTEGER NOT NULL, idempotency_key TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued', attempt_count INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL, available_at TEXT NOT NULL, lease_owner TEXT NOT NULL DEFAULT '',
            lease_expires_at TEXT NOT NULL DEFAULT '', current_result_version INTEGER,
            last_error_code TEXT NOT NULL DEFAULT '', last_error_message TEXT NOT NULL DEFAULT '',
            version INTEGER NOT NULL DEFAULT 1, started_at TEXT, finished_at TEXT,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            UNIQUE(requested_by, idempotency_key)
        );
        CREATE TABLE compute_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER NOT NULL, version INTEGER NOT NULL,
            result_json TEXT NOT NULL, metrics_json TEXT NOT NULL DEFAULT '{}', result_digest TEXT NOT NULL,
            created_by TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(task_id, version)
        );
        CREATE TABLE compute_interventions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER NOT NULL, actor TEXT NOT NULL,
            action TEXT NOT NULL, reason TEXT NOT NULL, before_json TEXT NOT NULL, after_json TEXT NOT NULL,
            batch_key TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL
        );
        """
    )
    raw.execute(
        "INSERT INTO compute_templates(id,code,name,algorithm,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,created_by,created_at,updated_at) VALUES(1,?,?,?,?,?,300,2,'administrator',?,?)",
        (TEMPLATE["code"], TEMPLATE["name"], TEMPLATE["algorithm"], json.dumps(TEMPLATE["parameter_schema"], sort_keys=True), json.dumps(TEMPLATE["default_parameters"], sort_keys=True), now, now),
    )
    raw.execute(
        "INSERT INTO compute_tasks(id,template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,created_at,updated_at) VALUES(1,1,'project-a','researcher-1',?,?,50,'legacy-shared-key','queued',0,2,?,?,?)",
        (json.dumps(parameters, sort_keys=True), digest(parameters), now, now, now),
    )
    raw.execute(
        "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(1,1,'{\"value\":1}','{}','r-digest','worker-1',?)",
        (now,),
    )
    raw.execute(
        "INSERT INTO compute_interventions(task_id,actor,action,reason,before_json,after_json,batch_key,created_at) VALUES(1,'administrator','priority','提级','{}','{}','',?)",
        (now,),
    )
    raw.commit()
    raw.close()

    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(db_path))
    from app.database import close_connection, get_connection, init_db
    close_connection()
    init_db()
    init_db()  # 迁移必须可重复执行
    connection = get_connection()
    table_sql = connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='compute_tasks'").fetchone()[0]
    assert "uq_compute_tasks_submission" in table_sql
    assert "UNIQUE(requested_by, idempotency_key)" not in table_sql
    legacy = connection.execute("SELECT * FROM compute_tasks WHERE id=1").fetchone()
    assert legacy["project_code"] == "project-a" and legacy["submitted_priority"] == 50
    assert connection.execute("SELECT COUNT(*) FROM compute_results WHERE task_id=1").fetchone()[0] == 1
    assert connection.execute("SELECT COUNT(*) FROM compute_interventions WHERE task_id=1").fetchone()[0] == 1
    assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    service = ComputeOperationsService(connection)
    repeated = service.submit(submit_payload("legacy-shared-key"))
    assert repeated["id"] == 1
    second_project = submit_payload("legacy-shared-key")
    second_project["project_code"] = "project-b"
    created = service.submit(second_project)
    assert created["id"] == 2 and created["project_code"] == "project-b"


def test_priority_capability_claim_and_result_version(client):
    create_template(client)
    low = client.post("/api/compute/tasks", json=submit_payload("priority-low", priority=10)).json()
    high = client.post("/api/compute/tasks", json=submit_payload("priority-high", priority=90)).json()
    no_match = client.post("/api/compute/tasks/claim", json={"worker_id": "w0", "capabilities": ["other"], "lease_seconds": 60})
    assert no_match.status_code == 200 and no_match.json()["task"] is None
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.status_code == 200
    assert claimed.json()["task"]["id"] == high["id"]
    completed = client.post(
        f"/api/compute/tasks/{high['id']}/complete",
        json={"worker_id": "w1", "result": {"value": 3.14}, "metrics": {"seconds": 2}},
    )
    assert completed.status_code == 200
    details = client.get(f"/api/compute/task-details/{high['id']}").json()
    assert details["status"] == "succeeded"
    assert details["current_result_version"] == 1
    assert len(details["results"]) == 1
    assert low["status"] == "queued"


def test_quota_cancel_retry_priority_and_batch_interventions(client):
    create_template(client)
    quota = client.put(
        "/api/compute/quotas?actor=administrator",
        json={"subject_type": "user", "subject_key": "limited", "max_queued": 1, "max_running": 1, "daily_submissions": 2},
    )
    assert quota.status_code == 200
    one = client.post("/api/compute/tasks", json=submit_payload("quota-one", user="limited")).json()
    blocked = client.post("/api/compute/tasks", json=submit_payload("quota-two", user="limited"))
    assert blocked.status_code == 409
    cancelled = client.post(f"/api/compute/tasks/{one['id']}/cancel", json={"actor": "administrator", "reason": "项目暂停"})
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
    retried = client.post(f"/api/compute/tasks/{one['id']}/retry", json={"actor": "administrator", "reason": "项目恢复", "priority": 95})
    assert retried.status_code == 200 and retried.json()["priority"] == 95
    other = client.post("/api/compute/tasks", json=submit_payload("batch-other", user="other-user")).json()
    batch = client.post(
        "/api/compute/tasks/batch",
        json={"task_ids": [one["id"], other["id"]], "operation": "priority", "actor": "administrator", "reason": "紧急算例", "priority": 99},
    )
    assert batch.status_code == 200
    assert len(batch.json()["succeeded"]) == 2
    details = client.get(f"/api/compute/task-details/{one['id']}").json()
    assert [item["action"] for item in details["interventions"]] == ["cancel", "retry", "priority"]


def test_failure_backoff_and_expired_lease_recovery(client):
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    first = service.submit(submit_payload("failure-000001"))
    claimed = service.claim("worker-a", ["solver-a"], 10)
    assert claimed and claimed["id"] == first["id"]
    failed = service.fail(first["id"], "worker-a", "numeric_error", "数值不收敛", True)
    assert failed["status"] == "queued"
    assert failed["available_at"] > failed["updated_at"]
    clock.advance(seconds=2)
    claimed_again = service.claim("worker-a", ["solver-a"], 10)
    assert claimed_again and claimed_again["attempt_count"] == 2
    clock.advance(seconds=11)
    recovered = service.recover_expired()
    assert recovered["exhausted"] == [first["id"]]
    details = service.get_task(first["id"])
    assert details["status"] == "failed"
    assert details["interventions"][-1]["action"] == "lease_recovery"
