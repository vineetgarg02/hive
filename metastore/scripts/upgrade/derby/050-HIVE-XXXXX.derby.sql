-- add a new column to support default value for DEFAULT constraint
ALTER TABLE "APP"."KEY_CONSTRAINTS" ADD COLUMN "DEFAULT_VALUE" VARCHAR(400);
