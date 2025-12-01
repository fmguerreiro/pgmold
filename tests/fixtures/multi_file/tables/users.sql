CREATE TABLE users (
    id BIGINT NOT NULL,
    email VARCHAR(255) NOT NULL,
    role user_role NOT NULL DEFAULT 'user',
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX users_email_idx ON users (email);
