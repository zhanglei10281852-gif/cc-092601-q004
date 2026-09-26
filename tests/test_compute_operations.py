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


def submit_payload(key: str, *, user: str = "researcher-1", priority: int = 50, project: str = "project-a", template: str = "solver-a") -> dict:
    return {
        "template_code": template,
        "project_code": project,
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


def test_cross_project_key_reuse_creates_independent_tasks(client):
    create_template(client)
    second_template = dict(TEMPLATE, code="solver-b", algorithm="solver-b")
    created = client.post("/api/compute/templates?actor=administrator", json=second_template)
    assert created.status_code == 201, created.text

    # 两个项目恰好复用同一个请求号，应得到彼此独立的任务
    project_a = client.post("/api/compute/tasks", json=submit_payload("request-shared-01", project="project-a"))
    project_b = client.post("/api/compute/tasks", json=submit_payload("request-shared-01", project="project-b"))
    assert project_a.status_code == project_b.status_code == 202
    assert project_a.json()["id"] != project_b.json()["id"]
    assert project_a.json()["project_code"] == "project-a"
    assert project_b.json()["project_code"] == "project-b"

    # 同一提交人、同一请求号但不同模板同样是独立提交
    other_template = client.post("/api/compute/tasks", json=submit_payload("request-shared-01", project="project-a", template="solver-b"))
    assert other_template.status_code == 202
    assert other_template.json()["id"] not in {project_a.json()["id"], project_b.json()["id"]}

    # 真正重复的请求始终指向同一记录
    replay_a = client.post("/api/compute/tasks", json=submit_payload("request-shared-01", project="project-a"))
    replay_b = client.post("/api/compute/tasks", json=submit_payload("request-shared-01", project="project-b"))
    assert replay_a.status_code == replay_b.status_code == 202
    assert replay_a.json()["id"] == project_a.json()["id"]
    assert replay_b.json()["id"] == project_b.json()["id"]

    # 项目账单与结果查询按项目各自隔离
    listed_a = client.get("/api/compute/tasks", params={"project_code": "project-a"}).json()["items"]
    listed_b = client.get("/api/compute/tasks", params={"project_code": "project-b"}).json()["items"]
    assert {item["id"] for item in listed_a} == {project_a.json()["id"], other_template.json()["id"]}
    assert {item["id"] for item in listed_b} == {project_b.json()["id"]}


def test_same_scope_conflict_on_parameter_or_priority_change(client):
    create_template(client)
    original = client.post("/api/compute/tasks", json=submit_payload("request-conflict-1", priority=40))
    assert original.status_code == 202

    changed_parameters = submit_payload("request-conflict-1", priority=40)
    changed_parameters["parameters"]["iterations"] = 200
    parameter_conflict = client.post("/api/compute/tasks", json=changed_parameters)
    assert parameter_conflict.status_code == 409
    error = parameter_conflict.json()["error"]
    assert error["code"] == "conflict"
    assert error["context"]["existing_task_id"] == original.json()["id"]
    assert error["context"]["differing_parameters"] == ["iterations"]
    assert error["context"]["project_code"] == "project-a"
    assert error["context"]["idempotency_key"] == "request-conflict-1"

    priority_conflict = client.post("/api/compute/tasks", json=submit_payload("request-conflict-1", priority=90))
    assert priority_conflict.status_code == 409
    error = priority_conflict.json()["error"]
    assert error["context"]["existing_task_id"] == original.json()["id"]
    assert error["context"]["existing_priority"] == 40
    assert error["context"]["provided_priority"] == 90

    # 冲突不影响完全一致的重复提交
    replay = client.post("/api/compute/tasks", json=submit_payload("request-conflict-1", priority=40))
    assert replay.status_code == 202
    assert replay.json()["id"] == original.json()["id"]


def test_replayed_batch_submission_does_not_consume_quota(client):
    create_template(client)
    quota = client.put(
        "/api/compute/quotas?actor=administrator",
        json={"subject_type": "user", "subject_key": "batch-user", "max_queued": 10, "max_running": 4, "daily_submissions": 2},
    )
    assert quota.status_code == 200
    first = client.post("/api/compute/tasks", json=submit_payload("batch-key-0001", user="batch-user"))
    second = client.post("/api/compute/tasks", json=submit_payload("batch-key-0002", user="batch-user"))
    assert first.status_code == second.status_code == 202

    # 当日提交配额已用尽，全新提交被拒绝
    blocked = client.post("/api/compute/tasks", json=submit_payload("batch-key-0003", user="batch-user"))
    assert blocked.status_code == 409

    # 但批量重放的提交仍返回原任务，不重复扣减配额
    replay_first = client.post("/api/compute/tasks", json=submit_payload("batch-key-0001", user="batch-user"))
    replay_second = client.post("/api/compute/tasks", json=submit_payload("batch-key-0002", user="batch-user"))
    assert replay_first.status_code == replay_second.status_code == 202
    assert replay_first.json()["id"] == first.json()["id"]
    assert replay_second.json()["id"] == second.json()["id"]

    tasks = client.get("/api/compute/tasks", params={"requested_by": "batch-user"}).json()["items"]
    assert len(tasks) == 2


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


LEGACY_COMPUTE_DDL = """
CREATE TABLE compute_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    parameter_schema_json TEXT NOT NULL,
    default_parameters_json TEXT NOT NULL DEFAULT '{}',
    max_runtime_seconds INTEGER NOT NULL CHECK(max_runtime_seconds > 0),
    max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE compute_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_id INTEGER NOT NULL REFERENCES compute_templates(id) ON DELETE RESTRICT,
    project_code TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    parameter_digest TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 50 CHECK(priority BETWEEN 0 AND 100),
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','cancel_requested','cancelled','succeeded','failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
    available_at TEXT NOT NULL,
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT NOT NULL DEFAULT '',
    current_result_version INTEGER,
    last_error_code TEXT NOT NULL DEFAULT '',
    last_error_message TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 1,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(requested_by, idempotency_key)
);
CREATE INDEX idx_compute_tasks_queue ON compute_tasks(status,priority DESC,available_at,created_at);
CREATE INDEX idx_compute_tasks_owner ON compute_tasks(requested_by,status,created_at);
CREATE TABLE compute_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES compute_tasks(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    metrics_json TEXT NOT NULL DEFAULT '{}',
    result_digest TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(task_id, version)
);
CREATE TABLE compute_interventions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES compute_tasks(id) ON DELETE CASCADE,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT NOT NULL,
    before_json TEXT NOT NULL,
    after_json TEXT NOT NULL,
    batch_key TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
"""


def test_legacy_idempotency_scope_migration(tmp_path, monkeypatch):
    from app.compute.service import digest
    from app.database import close_connection, get_connection, init_db

    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(tmp_path / "legacy.db"))
    close_connection()
    connection = get_connection()
    connection.executescript(LEGACY_COMPUTE_DDL)
    now = "2026-09-26T00:00:00+00:00"
    connection.execute(
        "INSERT INTO compute_templates(code,name,algorithm,version,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,active,created_by,created_at,updated_at) VALUES('solver-a','方程求解模板','solver-a',1,'{}','{}',300,2,1,'administrator',?,?)",
        (now, now),
    )
    template_id = connection.execute("SELECT id FROM compute_templates WHERE code='solver-a'").fetchone()[0]
    for project, key in (("project-a", "legacy-key-0001"), ("project-b", "legacy-key-0002")):
        connection.execute(
            "INSERT INTO compute_tasks(template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,created_at,updated_at) VALUES(?,?,?,'{}',?,50,?,'queued',0,2,?,?,?)",
            (template_id, project, "researcher-1", digest({}), key, now, now, now),
        )
    legacy_task_id = connection.execute("SELECT id FROM compute_tasks WHERE idempotency_key='legacy-key-0001'").fetchone()[0]
    connection.execute(
        "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,1,'{}','{}','digest','worker-1',?)",
        (legacy_task_id, now),
    )
    connection.execute(
        "INSERT INTO compute_interventions(task_id,actor,action,reason,before_json,after_json,batch_key,created_at) VALUES(?,'administrator','priority','提级','{}','{}','',?)",
        (legacy_task_id, now),
    )

    init_db()

    # 历史任务、结果版本与干预记录完整保留，外键仍然有效
    rows = connection.execute("SELECT id,project_code,idempotency_key FROM compute_tasks ORDER BY id").fetchall()
    assert [(row["project_code"], row["idempotency_key"]) for row in rows] == [("project-a", "legacy-key-0001"), ("project-b", "legacy-key-0002")]
    assert connection.execute("SELECT COUNT(*) FROM compute_results WHERE task_id=?", (legacy_task_id,)).fetchone()[0] == 1
    assert connection.execute("SELECT COUNT(*) FROM compute_interventions WHERE task_id=?", (legacy_task_id,)).fetchone()[0] == 1
    assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

    service = ComputeOperationsService(get_connection())

    # 迁移后历史作用域内的重复提交仍返回原任务
    legacy_payload = submit_payload("legacy-key-0001", project="project-a")
    legacy_payload["parameters"] = {}
    replayed = service.submit(legacy_payload)
    assert replayed["id"] == legacy_task_id

    # 同一提交人与请求号现在可以在另一个项目独立提交，且任务 id 继续递增
    cross_project = service.submit(dict(legacy_payload, project_code="project-c"))
    assert cross_project["id"] > legacy_task_id
    assert cross_project["project_code"] == "project-c"
    assert service.submit(dict(legacy_payload, project_code="project-c"))["id"] == cross_project["id"]

    # 迁移是幂等的，重复初始化不会再次重建
    init_db()
    assert connection.execute("SELECT COUNT(*) FROM compute_tasks").fetchone()[0] == 3
    close_connection()
