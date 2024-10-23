-- Connect to the PostgreSQL database
\connect -h 138.100.82.178 -p 5432 -d lectura -U lectura

-- Export the schema to a file
\! pg_dump -h 138.100.82.178 -p 5432 -U lectura -s -f schema_export.sql lectura
