cd: cd epochai/common/database

upgrade: alembic upgrade x
x = head -> upgrade all
x = file_name -> upgrade until that file
x = +1 -> upgrade by one

downgrade: alembic downgrade x
x = base -> downgrade all
x = file_name -> downgrade until that file
x = -1 -> downgrade by one

create new migration: alembic revision -m "file_name"

tell alembic what current head is: alembic stamp x
x = base -> base is nothing
x = file_name -> base is this file_name