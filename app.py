import os
import uuid
import logging
from os import getenv
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
from simple_salesforce import Salesforce

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Real job modules (run-once wrappers)
import prop_main_test
import contact_main

# Load variables from .env in the current working directory (if present)
load_dotenv()

app = FastAPI()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SALESFORCE_USERNAME = getenv("SF_USERNAME")
SALESFORCE_PASSWORD = getenv("SF_PASSWORD")
SALESFORCE_SECURITY_TOKEN = getenv("SF_TOKEN")
# SF_DOMAIN = getenv("SF_DOMAIN").strip().strip('"').strip("'")  # "test" for sandbox, "login" for production
SF_DOMAIN = (getenv("SF_DOMAIN", "test") or "test").strip().strip('"').strip("'")

from fastapi.responses import JSONResponse
import traceback

@app.exception_handler(Exception)
async def all_exception_handler(request, exc):
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={"error": str(exc), "type": exc.__class__.__name__}
    )

DUMMY_MODE = os.getenv("DUMMY_MODE", "false").lower() == "true"

# -------------------- Simple daily run limit (local dev) --------------------
RUN_LIMIT_PER_DAY = int(getenv("RUN_LIMIT_PER_DAY", "10") or "10")
RUN_COUNTER_PATH = Path(getenv("RUN_COUNTER_PATH", ".run_counts.json"))


def _today_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _load_run_counts() -> dict[str, Any]:
    if not RUN_COUNTER_PATH.exists():
        return {}
    try:
        return json.loads(RUN_COUNTER_PATH.read_text())
    except Exception:
        return {}


def _save_run_counts(data: dict[str, Any]) -> None:
    RUN_COUNTER_PATH.write_text(json.dumps(data, indent=2, sort_keys=True))


def assert_within_daily_limit(actor: str) -> dict[str, Any]:
    """Allow up to RUN_LIMIT_PER_DAY runs per UTC day per actor (email)."""
    data = _load_run_counts()
    day = _today_key()

    actor = actor or "unknown"
    data.setdefault(day, {})
    data[day].setdefault(actor, 0)

    if data[day][actor] >= RUN_LIMIT_PER_DAY:
        raise RuntimeError(
            f"Daily run limit reached for {actor}: {data[day][actor]}/{RUN_LIMIT_PER_DAY} on {day} (UTC)."
        )

    data[day][actor] += 1
    _save_run_counts(data)
    return {"day": day, "actor": actor, "count": data[day][actor], "limit": RUN_LIMIT_PER_DAY}

def connect_to_salesforce() -> Salesforce:
    """Establish connection to Salesforce org using username/password/security token."""
    missing = [k for k, v in {
        "SF_USERNAME": SALESFORCE_USERNAME,
        "SF_PASSWORD": SALESFORCE_PASSWORD,
        "SF_TOKEN": SALESFORCE_SECURITY_TOKEN,
    }.items() if not v]

    if missing:
        raise RuntimeError(
            f"Missing required environment variable(s): {', '.join(missing)}. "
            "Set SF_USERNAME, SF_PASSWORD, SF_TOKEN in your shell or in a .env file (and optionally SF_DOMAIN)."
        )

    try:
        # SF_DOMAIN should be "test" for sandbox, "login" for production
        return Salesforce(
            username=SALESFORCE_USERNAME,
            password=SALESFORCE_PASSWORD,
            security_token=SALESFORCE_SECURITY_TOKEN,
            domain=SF_DOMAIN,
        )
    except Exception as e:
        logger.error(f"Failed to connect to Salesforce: {e}")
        raise

class RunRequest(BaseModel):
    requestedBy: str | None = None
    source: str | None = "salesforce_list_button"

def run_real_script_once(sf: Salesforce, req: RunRequest) -> dict:
    """Run prop job once, then contacts job once ONLY if prop succeeds."""

    logger.info("Starting prop job")

    try:
        prop_fn = getattr(prop_main_test, "run_prop_job_once", None) or getattr(prop_main_test, "run_properties_job_once", None)
        if prop_fn is None:
            raise RuntimeError("prop_main_test is missing run_prop_job_once(sf) or run_properties_job_once(sf)")
        prop_result = prop_fn(sf)
    except Exception as e:
        logger.exception("Prop job raised exception")
        return {
            "status": "FAILED",
            "step": "prop",
            "success": False,
            "error": str(e),
        }

    if not prop_result.get("success", False):
        logger.warning("Prop job failed, skipping contacts job")
        return {
            "status": "FAILED",
            "step": "prop",
            "prop": prop_result,
            "contacts": None,
        }

    logger.info("Prop job succeeded, starting contacts job")

    try:
        contacts_fn = getattr(contact_main, "run_contacts_job_once", None)
        if contacts_fn is None:
            raise RuntimeError("contact_main is missing run_contacts_job_once(sf)")
        contacts_result = contacts_fn(sf)
    except Exception as e:
        logger.exception("Contacts job raised exception")
        return {
            "status": "FAILED",
            "step": "contacts",
            "prop": prop_result,
            "success": False,
            "error": str(e),
        }
    

    return {
        "status": "DONE",
        "success": True,
        "prop": prop_result,
        "contacts": contacts_result,
    }

@app.get("/ping")
def ping():
    return {"ok": True}

@app.post("/run")
def run(req: RunRequest):
    # Identify who is clicking (Salesforce passes requestedBy from Apex)
    actor = (req.requestedBy or "unknown").strip()

    # Optional: keep dummy mode for smoke tests
    if DUMMY_MODE:
        run_id = str(uuid.uuid4())[:8]
        limit_info = assert_within_daily_limit(actor)
        return {
            "status": "OK",
            "message": "Dummy mode working",
            "runId": run_id,
            "requestedBy": req.requestedBy,
            "source": req.source,
            "limit": limit_info,
        }

    # Enforce daily limit before doing any Salesforce work
    limit_info = assert_within_daily_limit(actor)

    sf = connect_to_salesforce()
    response = run_real_script_once(sf, req)
    response["limit"] = limit_info
    return response