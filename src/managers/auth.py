#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling authentication credentials."""

import logging

from core.models import Context
from core.workload import WorkloadBase
from literals import GROUP, USER

logger = logging.getLogger(__name__)


class AuthManager:
    """Manager for handling Kafka Connect `PropertyFileLoginModule`-style credential file stores.

    These files consist of several lines in the format `{USER}: {PASSWORD}`.
    """

    DELIMITER = ":"

    def __init__(self, context: Context, workload: WorkloadBase, store_path: str):
        self.context = context
        self.workload = workload
        self.store_path = store_path

    def _load_credentials(self) -> dict[str, str]:
        """Loads credentials from internal file store and returns a dict of username: password mappings."""
        raw = self.workload.read(self.store_path)
        credentials = {}
        for line in raw:
            parts = line.split(self.DELIMITER)
            if len(parts) == 2:
                credentials[parts[0].strip()] = parts[1].strip()

        return credentials

    def _save_credentials(self, cache: dict[str, str]) -> None:
        """Saves a map of credentials to the internal file store.

        Args:
            cache (dict[str, str]): a dict of username: password mapping to be saved to internal file store.
        """
        raw = [f"{username}{self.DELIMITER} {password}" for username, password in cache.items()]
        self.workload.write(content="\n".join(raw) + "\n", path=self.store_path)

        self.workload.exec(f"chown {USER}:{GROUP} {self.store_path}".split())
        self.workload.exec(["chmod", "770", self.store_path])

    def update(self, credentials: dict[str, str]) -> None:
        """Updates credentials in the internal file store.

        Args:
            credentials (dict[str, str]): a dict of username: password mapping to be updated.
        """
        cache = self._load_credentials() | credentials
        self._save_credentials(cache)

    def remove_user(self, username: str) -> None:
        """Removes a username from the internal file store."""
        cache = {u: p for u, p in self.credentials.items() if u != username}
        self._save_credentials(cache)

    @property
    def credentials(self) -> dict[str, str]:
        """Returns a dict mapping of username: password."""
        return self._load_credentials()
