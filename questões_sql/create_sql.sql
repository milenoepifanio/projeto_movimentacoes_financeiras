CREATE TABLE country (
  country VARCHAR(128),
  country_id UUID PRIMARY KEY
);

CREATE TABLE state (
  state VARCHAR(128),
  country_id UUID,
  state_id UUID PRIMARY KEY,
  FOREIGN KEY (country_id) REFERENCES country(country_id)
);

CREATE TABLE city (
  city VARCHAR(256),
  state_id UUID,
  city_id INT PRIMARY KEY,
  FOREIGN KEY (state_id) REFERENCES state(state_id)
);

CREATE TABLE d_month (
  month_id BIGINT PRIMARY KEY,
  action_month BIGINT
);

CREATE TABLE d_year (
  year_id INT PRIMARY KEY,
  action_year INT
);

CREATE TABLE d_week (
  week_id BIGINT PRIMARY KEY,
  action_week BIGINT
);

CREATE TABLE d_weekday (
  weekday_id BIGINT PRIMARY KEY,
  action_weekday VARCHAR(128)
);

CREATE TABLE d_time (
  time_id BIGINT PRIMARY KEY,
  action_timestamp TIMESTAMP,
  week_id BIGINT,
  month_id BIGINT,
  year_id BIGINT,
  weekday_id BIGINT,
  FOREIGN KEY (week_id) REFERENCES d_week(week_id),
  FOREIGN KEY (month_id) REFERENCES d_month(month_id),
  FOREIGN KEY (year_id) REFERENCES d_year(year_id),
  FOREIGN KEY (weekday_id) REFERENCES d_weekday(weekday_id)
);

CREATE TABLE customers (
  customer_id UUID PRIMARY KEY,
  first_name VARCHAR(128),
  last_name VARCHAR(128),
  customer_city BIGINT,
  cpf BIGINT,
  country_name VARCHAR(128),
  FOREIGN KEY (customer_city) REFERENCES city(city_id)
);

CREATE TABLE accounts (
  account_id UUID PRIMARY KEY,
  customer_id UUID,
  created_at TIMESTAMP,
  status VARCHAR(128),
  account_branch VARCHAR(128),
  account_check_digit VARCHAR(128),
  account_number VARCHAR(128),
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE transfer_ins (
  id UUID PRIMARY KEY,
  account_id UUID,
  amount FLOAT,
  transaction_requested_at BIGINT,
  transaction_completed_at BIGINT,
  status VARCHAR(128),
  FOREIGN KEY (account_id) REFERENCES accounts(account_id),
  FOREIGN KEY (transaction_requested_at) REFERENCES d_time(time_id),
  FOREIGN KEY (transaction_completed_at) REFERENCES d_time(time_id)
);

CREATE TABLE transfer_outs (
  id UUID PRIMARY KEY,
  account_id UUID,
  amount FLOAT,
  transaction_requested_at BIGINT,
  transaction_completed_at BIGINT,
  status VARCHAR(128),
  FOREIGN KEY (account_id) REFERENCES accounts(account_id),
  FOREIGN KEY (transaction_requested_at) REFERENCES d_time(time_id),
  FOREIGN KEY (transaction_completed_at) REFERENCES d_time(time_id)
);

CREATE TABLE pix_movements (
  id UUID PRIMARY KEY,
  account_id UUID,
  in_or_out VARCHAR(128),
  pix_amount FLOAT,
  pix_requested_at BIGINT,
  pix_completed_at BIGINT,
  status VARCHAR(128),
  FOREIGN KEY (account_id) REFERENCES accounts(account_id),
  FOREIGN KEY (pix_requested_at) REFERENCES d_time(time_id),
  FOREIGN KEY (pix_completed_at) REFERENCES d_time(time_id)
);