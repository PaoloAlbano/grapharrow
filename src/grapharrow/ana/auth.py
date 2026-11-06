"""Read and validate the ANA-compatible GraphArrow client file."""

import hashlib
import hmac
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, ValidationError


class AuthenticationError(ValueError):
    """The supplied bearer credential was rejected."""


class AuthNotConfigured(RuntimeError):
    """The server has no configured ANA clients."""


class Client(BaseModel):
    id: str = Field(min_length=1)
    api_key: str = Field(min_length=32, repr=False)
    profile: Literal["readonly", "readwrite"] = "readonly"


class ClientFile(BaseModel):
    clients: list[Client]


@dataclass(frozen=True)
class Principal:
    subject: str
    profile: str

    def permits(self, profile: str) -> bool:
        return profile == "readonly" or profile == self.profile == "readwrite"


class ApiKeyAuth:
    def __init__(self, clients: list[Client]) -> None:
        self._clients: dict[bytes, Principal] = {}
        ids = set()
        for client in clients:
            key = self._digest(client.api_key)
            if not client.id.strip() or client.id in ids or key in self._clients:
                raise ValueError("ANA client IDs and keys must be unique and nonempty")
            ids.add(client.id)
            self._clients[key] = Principal(client.id, client.profile)

    @classmethod
    def from_env(cls) -> "ApiKeyAuth":
        filename = os.getenv("ANA_AUTH_FILE")
        if not filename:
            return cls([])
        try:
            clients = ClientFile.model_validate_json(Path(filename).read_bytes()).clients
        except (OSError, ValidationError):
            raise ValueError("Invalid ANA authentication file") from None
        return cls(clients)

    @staticmethod
    def _digest(key: str) -> bytes:
        return hashlib.sha256(key.encode()).digest()

    def authenticate(self, header: str | None) -> Principal:
        if not self._clients:
            raise AuthNotConfigured("ANA authentication is not configured")
        scheme, _, key = (header or "").partition(" ")
        if scheme.lower() == "bearer" and key and not any(char.isspace() for char in key):
            digest = self._digest(key)
            for candidate, principal in self._clients.items():
                if hmac.compare_digest(candidate, digest):
                    return principal
        raise AuthenticationError("Invalid or missing ANA bearer token")
