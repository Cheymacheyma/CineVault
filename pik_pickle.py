"""Alias module for the Pik Pickle database layer."""

import pickle
import os

__all__ = ["load_db", "save_db"]

DB_FILE = "bigpickle.db"

def load_db():
    if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 0:
        with open(DB_FILE, "rb") as f:
            return pickle.load(f)
    return {}

def save_db(data):
    with open(DB_FILE, "wb") as f:
        pickle.dump(data, f)
