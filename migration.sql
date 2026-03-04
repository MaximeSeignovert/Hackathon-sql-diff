-- Migration SQL generee automatiquement
-- Cible: Postgres
-- Source -> Target

CREATE TABLE IF NOT EXISTS "product" (
  "id" integer,
  "name" text NOT NULL,
  "price" numeric NOT NULL,
  "stock" integer DEFAULT 0
);
ALTER TABLE "orders" ADD COLUMN IF NOT EXISTS "status" text DEFAULT 'pending';
ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "full_name" text DEFAULT 'anonymous';
DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'users' AND column_name = 'age' AND LOWER(data_type) <> LOWER('bigint')) THEN ALTER TABLE "users" ALTER COLUMN "age" TYPE bigint; END IF; END $$;
-- ATTENTION: changement de type potentiellement destructif sur users.created_at
DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'users' AND column_name = 'created_at' AND LOWER(data_type) <> LOWER('text')) THEN ALTER TABLE "users" ALTER COLUMN "created_at" TYPE text; END IF; END $$;
DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'users' AND column_name = 'created_at' AND column_default IS NOT NULL) THEN ALTER TABLE "users" ALTER COLUMN "created_at" DROP DEFAULT; END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email");