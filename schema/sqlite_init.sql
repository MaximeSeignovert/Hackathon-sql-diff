CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL,
  age BIGINT,
  full_name TEXT DEFAULT 'anonymous',
  created_at TEXT
);

CREATE UNIQUE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_created_at ON users(created_at);

CREATE TABLE orders (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  amount NUMERIC NOT NULL,
  status TEXT DEFAULT 'pending',
  FOREIGN KEY(user_id) REFERENCES users(id)
);