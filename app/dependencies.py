from __future__ import annotations

from typing import Annotated

from fastapi import Depends
from sqlalchemy import Connection

from app.db import get_conn

Conn = Annotated[Connection, Depends(get_conn)]
