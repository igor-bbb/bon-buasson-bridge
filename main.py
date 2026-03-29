# -*- coding: utf-8 -*-

from fastapi import FastAPI

app = FastAPI(title="vectra-core-v1")


# ===== MOCK DATA =====

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
            "loss": -18000,
        }
    },
    "sku": {
        ("Сененко", "2026-02", "VARUS"): [
            {
                "name": "Bon Buasson 2L Lemon",
                "finrez": -6000,
                "margin": -8.5,
                "business": 15.0,
                "gap": -23.5,
                "action": "вывести SKU из сети",
                "effect": 6000,
            },
            {
                "name": "Bon Buasson 2L Orange",
                "finrez": -4200,
                "margin": -6.2,
                "business": 15.0,
                "gap": -21.2,
                "action": "вывести SKU из сети",
                "effect": 4200,
            },
        ]
    },
}


# ===== SERVICE CHECK =====

@app.get("/")
def root():
    return {"status": "vectra running"}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "vectra-core-v1",
    }


# ===== LEVEL: MANAGER =====

@app.get("/manager")
def get_manager(manager: str, period: str):
    key = (manager, period)
    data = DATA["manager"].get(key)

    if not data:
        return {"error": "manager not found"}

    return {
        "level": "manager",
        "manager": manager,
        "period": period,
        "finrez": data["finrez"],
        "margin": data["margin"],
        "business": data["business"],
        "gap": data["gap"],
        "main_network": data["network"],
        "main_loss": data["loss"],
        "next": "network",
    }


# ===== LEVEL: NETWORK =====

@app.get("/network")
def get_network(manager: str, period: str, network: str):
    key = (manager, period, network)
    data = DATA["network"].get(key)

    if not data:
        return {"error": "network not found"}

    return {
        "level": "network",
        "manager": manager,
        "period": period,
        "network": network,
        "finrez": data["finrez"],
        "margin": data["margin"],
        "business": data["business"],
        "gap": data["gap"],
        "loss": data["loss"],
        "next": "sku",
    }


# ===== LEVEL: SKU =====

@app.get("/sku")
def get_sku(manager: str, period: str, network: str):
    key = (manager, period, network)
    data = DATA["sku"].get(key)

    if not data:
        return {"error": "sku not found"}

    return {
        "level": "sku",
        "manager": manager,
        "period": period,
        "network": network,
        "items": data,
    }


# ===== FULL FLOW =====

@app.get("/full")
def full_flow(manager: str, period: str):
    manager_key = (manager, period)
    manager_data = DATA["manager"].get(manager_key)

    if not manager_data:
        return {"error": "manager not found"}

    network_name = manager_data["network"]

    network_key = (manager, period, network_name)
    network_data = DATA["network"].get(network_key)

    sku_key = (manager, period, network_name)
    sku_data = DATA["sku"].get(sku_key)

    return {
        "manager": {
            "name": manager,
            "period": period,
            "finrez": manager_data["finrez"],
            "margin": manager_data["margin"],
            "business": manager_data["business"],
            "gap": manager_data["gap"],
            "main_network": manager_data["network"],
            "main_loss": manager_data["loss"],
        },
        "network": {
            "name": network_name,
            "finrez": network_data["finrez"] if network_data else None,
            "margin": network_data["margin"] if network_data else None,
            "business": network_data["business"] if network_data else None,
            "gap": network_data["gap"] if network_data else None,
            "loss": network_data["loss"] if network_data else None,
        },
        "sku": sku_data if sku_data else [],
    }
