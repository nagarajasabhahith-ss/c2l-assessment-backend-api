# Fix Migration Issue

The database tables already exist (created via SQLAlchemy's `create_all()`), but Alembic doesn't know about them.

## Solution

Stamp the database with the current revision, then run the new migration:

```bash
# Step 1: Stamp database with initial revision (001_initial)
docker-compose exec backend alembic stamp 001_initial

# Step 2: Run the new migration (002_add_complexity_fields)
docker-compose exec backend alembic upgrade head
```

Or use the make command:
```bash
make db-stamp
make db-migrate
```
