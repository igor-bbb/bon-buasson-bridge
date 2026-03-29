# -*- coding: utf-8 -*-
from fastapi import FastAPI
from typing import Optional

app = FastAPI(title="vectra-core-v1")


# ===== MOCK DATA (вместо Google Sheets пока) =====

DATA = {
    "manager": {
        ("Сененко", "2026-02"): {
            "finrez": -46583,
            "margin": -7.3,
            "business": 10.0,
            "gap": -17.3,
            "network": "VARUS",
            "loss": -8000,
        }
    },
    "network": {
        ("Сененко", "2026-02", "VARUS"): {
            "finrez": -18000,
            "margin": -5.1,
            "business": 10.0,
            "gap": -15.1,
        }
    },
    "sku": {
        ("Сененко", "2026-02", "VARUS"): [
            {
                "name": "Bon Buasson 2L Lemon",
                "finrez": -6000,
                "margin": -8.5,
            },
            {
                "name": "Bon Buasson 2L Orange",
                "finrez": -4200,
                "margin": -6.2,
            },
        ]
    },
}


# ===== HEALTH =====

@app.get("/health")
def health():
    return {"status": "ok", "service": "vectra-core-v1"}


# ===== VECTRA ENTRY =====

@app.get("/manager")
def get_manager(manager: str, period: str):
    key = (manager, period)
    data = DATA["manager"].get(key)

    if not data:
        return {"error": "no data"}

    return {
        "level": "manager",
        "manager": manager,
        "period": period,
        "finrez": data["finrez"],
        "margin": data["margin"],
        "business": data["business"],
        "gap": data["gap"],
        "main_network": data["network"],
        "next": "network",
    }


@app.get("/network")
def get_network(manager: str, period: str, network: str):
    key = (manager, period, network)
    data = DATA["network"].get(key)

    if not data:
        return {"error": "no data"}

    return {
        "level": "network",
        "network": network,
        "finrez": data["finrez"],
        "margin": data["margin"],
        "business": data["business"],
        "gap": data["gap"],
        "next": "sku",
    }


@app.get("/sku")
def get_sku(manager: str, period: str, network: str):
    key = (manager, period, network)
    data = DATA["sku"].get(key)

    if not data:
        return {"error": "no data"}

    return {
        "level": "sku",
        "items": data,
    }
