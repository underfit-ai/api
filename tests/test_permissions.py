from __future__ import annotations

from uuid import UUID

import pytest
from fastapi import HTTPException

import underfit_api.db as db
from tests.conftest import CreateUser
from underfit_api.permissions import (
    can_view_project,
    require_account_admin,
    require_project_contributor,
    require_project_viewer,
)
from underfit_api.repositories import organization_members as org_members_repo
from underfit_api.repositories import organizations as orgs_repo
from underfit_api.repositories import project_collaborators as collab_repo
from underfit_api.repositories import projects as projects_repo


@pytest.fixture
def seed_project(create_user: CreateUser) -> tuple[UUID, UUID, UUID]:
    with db.engine.begin() as conn:
        owner = create_user("owner@example.com", "owner")
        outsider = create_user("outsider@example.com", "outsider")
        project = projects_repo.create(conn, owner.id, "private-proj", None, "private")
    return owner.id, outsider.id, project.id


def test_require_project_viewer_status_codes(seed_project: tuple[UUID, UUID, UUID]) -> None:
    owner_id, outsider_id, project_id = seed_project
    with db.engine.begin() as conn:
        with pytest.raises(HTTPException) as unauth:
            require_project_viewer(conn, project_id, None)
        assert unauth.value.status_code == 401

        with pytest.raises(HTTPException) as forbidden:
            require_project_viewer(conn, project_id, outsider_id)
        assert forbidden.value.status_code == 403

        require_project_viewer(conn, project_id, owner_id)


def test_collaborator_and_contributor_allowed(seed_project: tuple[UUID, UUID, UUID], create_user: CreateUser) -> None:
    owner_id, _, project_id = seed_project
    with db.engine.begin() as conn:
        collaborator = create_user("collab@example.com", "collab")
        collab_repo.add(conn, project_id, collaborator.id)
        require_project_viewer(conn, project_id, collaborator.id)
        require_project_contributor(conn, project_id, collaborator.id, account_id=owner_id, account_type="USER")


def test_org_admin_can_access_project(create_user: CreateUser) -> None:
    with db.engine.begin() as conn:
        admin = create_user("admin@example.com", "admin")
        member = create_user("member@example.com", "member")
        org = orgs_repo.create(conn, "myorg", "My Org")
        org_members_repo.add_member(conn, org.id, admin.id, "ADMIN")
        org_members_repo.add_member(conn, org.id, member.id, "MEMBER")
        project = projects_repo.create(conn, org.id, "org-proj", None, "private")

        require_account_admin(conn, org.id, "ORGANIZATION", admin.id)
        require_project_viewer(conn, project.id, admin.id)
        require_project_contributor(conn, project.id, admin.id, account_id=org.id, account_type="ORGANIZATION")

        with pytest.raises(HTTPException) as not_admin:
            require_account_admin(conn, org.id, "ORGANIZATION", member.id)
        assert not_admin.value.status_code == 403


def test_can_view_project_respects_public_visibility(create_user: CreateUser) -> None:
    with db.engine.begin() as conn:
        owner = create_user("o@example.com", "o")
        visitor = create_user("v@example.com", "v")
        public_project = projects_repo.create(conn, owner.id, "public-proj", None, "public")
        private_project = projects_repo.create(conn, owner.id, "private-proj2", None, "private")

        assert can_view_project(conn, public_project.id, None) is True
        assert can_view_project(conn, private_project.id, None) is False
        assert can_view_project(conn, private_project.id, visitor.id) is False
