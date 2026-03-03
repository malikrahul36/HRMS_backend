import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, field_validator

load_dotenv()

app = FastAPI(title="HRMS Lite API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


DB_FILE = "hrms_lite.db"


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id TEXT UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                department TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS attendance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id TEXT NOT NULL REFERENCES employees(employee_id) ON DELETE CASCADE,
                date DATE NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('Present', 'Absent')),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (employee_id, date)
            );
        """)
        conn.commit()


@app.on_event("startup")
def startup():
    init_db()


# ── Schemas ────────────────────────────────────────────────

class EmployeeCreate(BaseModel):
    employee_id: str
    full_name: str
    email: EmailStr
    department: str

    @field_validator("employee_id", "full_name", "department", mode="before")
    @classmethod
    def not_empty(cls, v: str) -> str:
        cleaned = (v or "").strip()
        if not cleaned:
            raise ValueError("Field cannot be blank")
        return cleaned


class AttendanceCreate(BaseModel):
    employee_id: str
    date: date
    status: str

    @field_validator("status", mode="before")
    @classmethod
    def valid_status(cls, v: str) -> str:
        if v not in ("Present", "Absent"):
            raise ValueError("Status must be 'Present' or 'Absent'")
        return v


# ── Employees ──────────────────────────────────────────────

@app.get("/employees")
def list_employees():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                e.*,
                COUNT(CASE WHEN a.status = 'Present' THEN 1 END) AS present_days,
                COUNT(a.id) AS total_days
            FROM employees e
            LEFT JOIN attendance a ON e.employee_id = a.employee_id
            GROUP BY e.id
            ORDER BY e.created_at DESC
        """)
        return [dict(row) for row in cur.fetchall()]


@app.post("/employees", status_code=201)
def create_employee(payload: EmployeeCreate):
    with get_db() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO employees (employee_id, full_name, email, department)
                VALUES (?, ?, ?, ?)
            """, (payload.employee_id, payload.full_name, payload.email, payload.department))

            last_id = cur.lastrowid
            cur.execute("SELECT * FROM employees WHERE id = ?", (last_id,))
            row = cur.fetchone()
            conn.commit()
            return dict(row)

        except sqlite3.IntegrityError as e:
            conn.rollback()
            msg = str(e)
            if "employee_id" in msg:
                raise HTTPException(400, "Employee ID already exists")
            if "email" in msg:
                raise HTTPException(400, "Email already registered")
            raise HTTPException(400, msg)


@app.get("/employees/{employee_id}")
def get_employee(employee_id: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM employees WHERE employee_id = ?", (employee_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Employee not found")
        return dict(row)


@app.delete("/employees/{employee_id}", status_code=200)
def delete_employee(employee_id: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM employees WHERE employee_id = ? RETURNING employee_id",
            (employee_id,)
        )
        row = cur.fetchone()
        if row:
            conn.commit()
            return {"message": f"Employee {employee_id} deleted successfully"}
        raise HTTPException(404, "Employee not found")


# ── Attendance ─────────────────────────────────────────────

@app.post("/attendance", status_code=201)
def mark_attendance(payload: AttendanceCreate):
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM employees WHERE employee_id = ?", (payload.employee_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Employee not found")

        try:
            cur.execute("""
                INSERT INTO attendance (employee_id, date, status)
                VALUES (?, ?, ?)
                ON CONFLICT(employee_id, date)
                DO UPDATE SET status = excluded.status
                RETURNING *
            """, (payload.employee_id, payload.date, payload.status))

            row = cur.fetchone()
            conn.commit()
            return dict(row)

        except Exception as e:
            conn.rollback()
            raise HTTPException(500, str(e))


@app.get("/attendance/{employee_id}")
def get_attendance(
    employee_id: str,
    from_date: Optional[date] = Query(None, alias="from"),
    to_date: Optional[date] = Query(None, alias="to"),
):
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute("SELECT * FROM employees WHERE employee_id = ?", (employee_id,))
        emp = cur.fetchone()
        if not emp:
            raise HTTPException(404, "Employee not found")

        query = "SELECT * FROM attendance WHERE employee_id = ?"
        params = [employee_id]

        if from_date:
            query += " AND date >= ?"
            params.append(from_date)
        if to_date:
            query += " AND date <= ?"
            params.append(to_date)

        query += " ORDER BY date DESC"

        cur.execute(query, params)
        rows = cur.fetchall()

        return {
            "employee": dict(emp),
            "records": [dict(r) for r in rows],
            "present_days": sum(1 for r in rows if r["status"] == "Present"),
            "total_days": len(rows),
        }


# ── Dashboard ──────────────────────────────────────────────

@app.get("/dashboard")
def dashboard():
    today = date.today()
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS total_employees FROM employees")
        total_employees = cur.fetchone()["total_employees"]

        cur.execute(
            "SELECT COUNT(*) AS present_today FROM attendance WHERE date = ? AND status = 'Present'",
            (today,)
        )
        present_today = cur.fetchone()["present_today"]

        cur.execute(
            "SELECT COUNT(*) AS absent_today FROM attendance WHERE date = ? AND status = 'Absent'",
            (today,)
        )
        absent_today = cur.fetchone()["absent_today"]

        cur.execute("""
            SELECT e.full_name, e.employee_id, e.department,
                   COUNT(CASE WHEN a.status = 'Present' THEN 1 END) AS present_days
            FROM employees e
            LEFT JOIN attendance a ON e.employee_id = a.employee_id
            GROUP BY e.id
            ORDER BY present_days DESC
            LIMIT 5
        """)
        top_employees = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT department,
                   COUNT(DISTINCT e.employee_id) AS headcount
            FROM employees e
            GROUP BY department
            ORDER BY headcount DESC
        """)
        dept_breakdown = [dict(r) for r in cur.fetchall()]

        return {
            "total_employees": total_employees,
            "present_today": present_today,
            "absent_today": absent_today,
            "not_marked_today": total_employees - present_today - absent_today,
            "top_employees": top_employees,
            "dept_breakdown": dept_breakdown,
        }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat()
    }