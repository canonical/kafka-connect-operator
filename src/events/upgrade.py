# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Connect in-place upgrades."""

import logging
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.upgrade import (
    ClusterNotReadyError,
    DataUpgrade,
    DependencyModel,
    UpgradeGrantedEvent,
)
from pydantic import BaseModel
from typing_extensions import override

if TYPE_CHECKING:
    from charm import ConnectCharm

logger = logging.getLogger(__name__)

ROLLBACK_INSTRUCTIONS = """Unit failed to upgrade and requires manual rollback to previous stable version.
    1. Re-run `pre-upgrade-check` action on the leader unit to enter 'recovery' state
    2. Run `juju refresh` to the previously deployed charm revision
"""


class ConnectDependencyModel(BaseModel):
    """Model for Connect Operator dependencies."""

    connect_service: DependencyModel


class ConnectUpgrade(DataUpgrade):
    """Implementation of :class:`DataUpgrade` overrides for in-place upgrades."""

    def __init__(self, charm: "ConnectCharm", **kwargs) -> None:
        super().__init__(charm, **kwargs)
        self.charm: "ConnectCharm" = charm

    @property
    def idle(self) -> bool:
        """Checks if cluster state is idle.

        Returns:
            True if cluster state is idle. Otherwise False
        """
        return not bool(self.upgrade_stack)

    @property
    def current_version(self) -> str:
        """Get current Connect version."""
        dependency_model: DependencyModel = getattr(self.dependency_model, "connect_service")
        return dependency_model.version

    def post_upgrade_check(self) -> None:
        """Runs necessary checks validating the unit is in a healthy state after upgrade."""
        self.pre_upgrade_check()

    @override
    def pre_upgrade_check(self) -> None:
        default_message = "Pre-upgrade check failed and cannot safely upgrade"
        if not self.charm.connect_manager.healthy:
            raise ClusterNotReadyError(message=default_message, cause="Cluster is not healthy")

    @override
    def build_upgrade_stack(self) -> list[int]:
        upgrade_stack = []
        units = set([self.charm.unit] + list(self.charm.context.peer_relation.units))  # type: ignore[reportOptionalMemberAccess]
        for unit in units:
            upgrade_stack.append(int(unit.name.split("/")[-1]))

        return upgrade_stack

    @override
    def log_rollback_instructions(self) -> None:
        logger.critical(ROLLBACK_INSTRUCTIONS)

    @override
    def _on_upgrade_granted(self, event: UpgradeGrantedEvent) -> None:
        self.charm.workload.stop()

        if not self.charm.workload.install():
            logger.error("Unable to install Snap")
            self.set_unit_failed()
            return

        self.apply_backwards_compatibility_fixes(event)

        logger.info(f"{self.charm.unit.name} upgrading service...")
        self.charm.context.worker_unit.should_restart = True
        self.charm.on.config_changed.emit()

        try:
            logger.debug("Running post-upgrade check...")
            self.post_upgrade_check()

            logger.debug("Marking unit completed...")
            self.set_unit_completed()

            # ensures leader gets it's own relation-changed when it upgrades
            if self.charm.unit.is_leader():
                logger.debug("Re-emitting upgrade-changed on leader...")
                self.on_upgrade_changed(event)
                # If idle run peer config_changed

        except ClusterNotReadyError as e:
            logger.error(e.cause)
            self.set_unit_failed()

    def apply_backwards_compatibility_fixes(self, _: UpgradeGrantedEvent) -> None:
        """A range of functions needed for backwards compatibility."""
        logger.info("Applying upgrade fixes")
        pass
