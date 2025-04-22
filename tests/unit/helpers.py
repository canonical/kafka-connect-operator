#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.


from ops.testing import PeerRelation, Relation, State


def get_relation(state: State, relation_name: str) -> Relation | PeerRelation:
    """Returns the [Peer]Relation object from the provided State object."""
    for relation in state.relations:
        if relation.endpoint == relation_name:
            return relation

    raise Exception(f"Relation {relation_name} not found in {state}")
